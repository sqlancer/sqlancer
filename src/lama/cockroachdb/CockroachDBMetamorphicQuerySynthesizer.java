package lama.cockroachdb;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import lama.IgnoreMeException;
import lama.Query;
import lama.QueryAdapter;
import lama.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;
import lama.cockroachdb.CockroachDBSchema.CockroachDBColumn;
import lama.cockroachdb.CockroachDBSchema.CockroachDBDataType;
import lama.cockroachdb.CockroachDBSchema.CockroachDBTables;
import lama.cockroachdb.ast.CockroachDBColumnReference;
import lama.cockroachdb.ast.CockroachDBExpression;
import lama.cockroachdb.ast.CockroachDBSelect;

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
	}

	public void check() throws SQLException {
		CockroachDBTables tables = globalState.getSchema().getRandomTableNonEmptyTables();
		gen = new CockroachDBExpressionGenerator(globalState).setColumns(tables.getColumns());
		CockroachDBExpression whereCondition = gen.generateExpression(CockroachDBDataType.BOOL.get());
		int optimizableCount = getOptimizableResult(globalState.getConnection(), whereCondition, tables, errors);
		if (optimizableCount == -1) {
			throw new IgnoreMeException();
		}
		int nonOptimizableCount = getNonOptimizedResult(globalState.getConnection(), whereCondition, tables, errors);
		if (nonOptimizableCount == -1) {
			throw new IgnoreMeException();
		}
		if (optimizableCount != nonOptimizableCount) {
			globalState.getState().queryString = optimizableQueryString + ";\n" + unoptimizedQuery + ";";
			throw new AssertionError(CockroachDBVisitor.asString(whereCondition));
		}
	}

	private int getOptimizableResult(Connection con, CockroachDBExpression whereCondition, CockroachDBTables tables,
			Set<String> errors) throws SQLException {
		CockroachDBSelect select = new CockroachDBSelect();
		CockroachDBColumn c = new CockroachDBColumn("*", null, false, false);
		select.setColumns(Arrays.asList(new CockroachDBColumnReference(c)));
		select.setFromTables(tables.getTables());
		select.setWhereCondition(whereCondition);
//		select.setOrderByTerms(gen.getOrderingTerms());
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
			while (rs.next()) {
				count++;
			}
//			rs.next();
//			count = rs.getInt(1);
		} catch (Exception e) {
			throw new AssertionError(s, e);
		}
		return count;
	}

	private int getNonOptimizedResult(Connection con, CockroachDBExpression whereCondition, CockroachDBTables tables,
			Set<String> errors) throws SQLException {
//		CockroachDBSelect select = new CockroachDBSelect();
//		select.setColumns(Arrays.asList(new CockroachDBAggregate(CockroachDBAggregateFunction.SUM, null)));
//		select.setFromTables(Arrays.asList(table));
//		select.setWhereCondition(whereCondition);

		String s = "SELECT SUM(count) FROM (SELECT CAST(" + CockroachDBVisitor.asString(whereCondition)
				+ " IS TRUE AS INT) as count FROM "
				+ tables.getTables().stream().map(t -> t.getName()).collect(Collectors.joining(", ")) + ")";
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
		}
		return count;
	}

}
