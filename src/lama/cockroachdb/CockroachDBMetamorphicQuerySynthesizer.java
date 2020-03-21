package lama.cockroachdb;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import lama.IgnoreMeException;
import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import lama.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;
import lama.cockroachdb.CockroachDBSchema.CockroachDBColumn;
import lama.cockroachdb.CockroachDBSchema.CockroachDBDataType;
import lama.cockroachdb.CockroachDBSchema.CockroachDBTables;
import lama.cockroachdb.ast.CockroachDBColumnReference;
import lama.cockroachdb.ast.CockroachDBExpression;
import lama.cockroachdb.ast.CockroachDBJoin;
import lama.cockroachdb.ast.CockroachDBJoin.OuterType;
import lama.cockroachdb.ast.CockroachDBSelect;
import lama.cockroachdb.ast.CockroachDBTableReference;

public class CockroachDBMetamorphicQuerySynthesizer {

	private final CockroachDBGlobalState globalState;
	private final Set<String> errors = new HashSet<>();
	private String optimizableQueryString;
	private String unoptimizedQuery;
	private CockroachDBExpressionGenerator gen;

	public CockroachDBMetamorphicQuerySynthesizer(CockroachDBGlobalState globalState) {
		this.globalState = globalState;
		CockroachDBErrors.addExpressionErrors(errors);
		CockroachDBErrors.addTransactionErrors(errors);
		errors.add("unable to vectorize execution plan"); // SET vectorize=experimental_always;
		errors.add(" mismatched physical types at index"); // SET vectorize=experimental_always;
		
	}

	public void check() throws SQLException {
		CockroachDBTables tables = globalState.getSchema().getRandomTableNonEmptyTables();
		List<CockroachDBTableReference> tableL = tables.getTables().stream().map(t -> new CockroachDBTableReference(t)).collect(Collectors.toList());
		List<CockroachDBTableReference> tableList = CockroachDBCommon.getTableReferences(tableL);
		gen = new CockroachDBExpressionGenerator(globalState).setColumns(tables.getColumns());
		List<CockroachDBJoin> joinExpressions = getJoins(tableList, globalState);
		CockroachDBExpression whereCondition = gen.generateExpression(CockroachDBDataType.BOOL.get());
		CockroachDBExpression havingCondition = null;
		if (Randomly.getBoolean()) {
			havingCondition = gen.generateExpression(CockroachDBDataType.BOOL.get());
		}
		int optimizableCount = getOptimizableResult(globalState.getConnection(), whereCondition, havingCondition, tableList, errors, joinExpressions);
		if (optimizableCount == -1) {
			throw new IgnoreMeException();
		}
		int nonOptimizableCount = getNonOptimizedResult(globalState.getConnection(), whereCondition, havingCondition, tableList, errors, joinExpressions);
		if (nonOptimizableCount == -1) {
			throw new IgnoreMeException();
		}
		if (optimizableCount != nonOptimizableCount) {
			globalState.getState().queryString = optimizableQueryString + ";\n" + unoptimizedQuery + ";";
			throw new AssertionError(CockroachDBVisitor.asString(whereCondition));
		}
	}

	public static List<CockroachDBJoin> getJoins(List<CockroachDBTableReference> tableList, CockroachDBGlobalState globalState) throws AssertionError {
		List<CockroachDBJoin> joinExpressions = new ArrayList<>();
		while (tableList.size() >= 2 && Randomly.getBoolean()) {
			CockroachDBTableReference leftTable = tableList.remove(0);
			CockroachDBTableReference rightTable = tableList.remove(0);
			List<CockroachDBColumn> columns = new ArrayList<>(leftTable.getTable().getColumns());
			columns.addAll(rightTable.getTable().getColumns());
			CockroachDBExpressionGenerator joinGen = new CockroachDBExpressionGenerator(globalState).setColumns(columns);
			switch (CockroachDBJoin.JoinType.getRandom()) {
			case INNER:
				joinExpressions.add(new CockroachDBJoin(leftTable, rightTable, CockroachDBJoin.JoinType.INNER, joinGen.generateExpression(CockroachDBDataType.BOOL.get())));
				break;
			case NATURAL:
				joinExpressions.add(CockroachDBJoin.createNaturalJoin(leftTable, rightTable));
				break;
			case CROSS:
				joinExpressions.add(CockroachDBJoin.createCrossJoin(leftTable, rightTable));
				break;
			case OUTER:
				joinExpressions.add(CockroachDBJoin.createOuterJoin(leftTable, rightTable, OuterType.getRandom(), joinGen.generateExpression(CockroachDBDataType.BOOL.get())));
				break;
			default:
				throw new AssertionError();
			}
		}
		return joinExpressions;
	}

	private int getOptimizableResult(Connection con, CockroachDBExpression whereCondition, CockroachDBExpression havingCondition, List<CockroachDBTableReference> tableList,
			Set<String> errors, List<CockroachDBJoin> joinExpressions) throws SQLException {
		CockroachDBSelect select = new CockroachDBSelect();
		CockroachDBColumn c = new CockroachDBColumn("COUNT(*)", null, false, false);
		select.setColumns(Arrays.asList(new CockroachDBColumnReference(c)));
		select.setFromTables(tableList);
		select.setWhereCondition(whereCondition);
		select.setJoinList(joinExpressions);
		if (Randomly.getBooleanWithRatherLowProbability() && false) {
			select.setOrderByTerms(gen.getOrderingTerms());
		}
		String s = CockroachDBVisitor.asString(select);
		if (globalState.getOptions().logEachSelect()) {
			globalState.getLogger().writeCurrent(s);
		}
		this.optimizableQueryString = s;
		int count = 0;
		Query q = new QueryAdapter(s, errors);
		try (ResultSet rs = q.executeAndGet(con)) {
			if (rs == null) {
				return -1;
			}
			if (rs.next()) {
				count = rs.getInt(1);
			}
//			rs.next();
//			count = rs.getInt(1);
		} catch (Exception e) {
			throw new AssertionError(s, e);
		}
		return count;
	}

	private int getNonOptimizedResult(Connection con, CockroachDBExpression whereCondition, CockroachDBExpression havingCondition, List<CockroachDBTableReference> tableList,
			Set<String> errors, List<CockroachDBJoin> joinList) throws SQLException {
//		CockroachDBSelect select = new CockroachDBSelect();
//		select.setColumns(Arrays.asList(new CockroachDBAggregate(CockroachDBAggregateFunction.SUM, null)));
//		select.setFromTables(Arrays.asList(table));
//		select.setWhereCondition(whereCondition);

		String fromString = tableList.stream().map(t -> t.getTable().getName()).collect(Collectors.joining(", "));
		if (!tableList.isEmpty() && !joinList.isEmpty()) {
			fromString += ", ";
		}
		String s = "SELECT SUM(count) FROM (SELECT CAST(" + CockroachDBVisitor.asString(whereCondition)
				+ " IS TRUE AS INT) as count FROM "
				+ fromString + 
				" " + joinList.stream().map(j -> CockroachDBVisitor.asString(j)).collect(Collectors.joining(", ")) + ")";
		if (globalState.getOptions().logEachSelect()) {
			globalState.getLogger().writeCurrent(s);
		}
		this.unoptimizedQuery = s;
		int count = 0;
		Query q = new QueryAdapter(s, errors);
		try (ResultSet rs = q.executeAndGet(con)) {
			if (rs == null) {
				return -1;
			}
			rs.next();
			count = rs.getInt(1);
		} catch (Exception e) {
			throw new AssertionError(s, e);
		}
		return count;
	}

}
