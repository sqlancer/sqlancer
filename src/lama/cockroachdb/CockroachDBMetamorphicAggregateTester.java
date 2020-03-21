package lama.cockroachdb;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.postgresql.util.PSQLException;

import lama.DatabaseProvider;
import lama.IgnoreMeException;
import lama.QueryAdapter;
import lama.Randomly;
import lama.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;
import lama.cockroachdb.CockroachDBSchema.CockroachDBDataType;
import lama.cockroachdb.CockroachDBSchema.CockroachDBTables;
import lama.cockroachdb.ast.CockroachDBAggregate;
import lama.cockroachdb.ast.CockroachDBAggregate.CockroachDBAggregateFunction;
import lama.cockroachdb.ast.CockroachDBAlias;
import lama.cockroachdb.ast.CockroachDBCast;
import lama.cockroachdb.ast.CockroachDBExpression;
import lama.cockroachdb.ast.CockroachDBJoin;
import lama.cockroachdb.ast.CockroachDBNotOperation;
import lama.cockroachdb.ast.CockroachDBSelect;
import lama.cockroachdb.ast.CockroachDBTableReference;
import lama.cockroachdb.ast.CockroachDBUnaryPostfixOperation;
import lama.cockroachdb.ast.CockroachDBUnaryPostfixOperation.CockroachDBUnaryPostfixOperator;

public class CockroachDBMetamorphicAggregateTester {

	private CockroachDBGlobalState state;
	private final Set<String> errors = new HashSet<>();
	private CockroachDBExpressionGenerator gen;

	public CockroachDBMetamorphicAggregateTester(CockroachDBGlobalState state) {
		this.state = state;
		CockroachDBErrors.addExpressionErrors(errors);
		// https://github.com/cockroachdb/cockroach/issues/46122
		errors.add("zero length schema unsupported");
		// https://github.com/cockroachdb/cockroach/issues/46123
		errors.add("input to aggregatorBase is not an execinfra.OpNode");

		errors.add("interface conversion: coldata.column");
		errors.add("float out of range");
	}

	public void check() throws SQLException {
		CockroachDBSchema s = state.getSchema();
		CockroachDBTables targetTables = s.getRandomTableNonEmptyTables();
		gen = new CockroachDBExpressionGenerator(state).setColumns(targetTables.getColumns());
		CockroachDBSelect select = new CockroachDBSelect();
		CockroachDBAggregateFunction windowFunction = Randomly
				.fromOptions(CockroachDBAggregate.CockroachDBAggregateFunction.getRandomMetamorphicOracle());
		CockroachDBAggregate aggregate = gen.generateWindowFunction(windowFunction);
		List<CockroachDBExpression> fetchColumns = new ArrayList<>();
		fetchColumns.add(aggregate);
		while (Randomly.getBooleanWithRatherLowProbability()) {
			fetchColumns.add(gen.generateAggregate());
		}
		select.setColumns(Arrays.asList(aggregate));
		List<CockroachDBTableReference> tableList = targetTables.getTables().stream()
				.map(t -> new CockroachDBTableReference(t)).collect(Collectors.toList());
		List<CockroachDBTableReference> from = CockroachDBCommon.getTableReferences(tableList);
		if (Randomly.getBooleanWithRatherLowProbability()) {
			select.setJoinList(CockroachDBMetamorphicQuerySynthesizer.getJoins(from, state));
		}
		select.setFromTables(from);
		if (Randomly.getBooleanWithRatherLowProbability()) {
			select.setOrderByTerms(gen.getOrderingTerms());
		}
		String originalQuery = CockroachDBVisitor.asString(select);

		CockroachDBExpression whereClause = gen.generateExpression(CockroachDBDataType.BOOL.get());
		CockroachDBNotOperation negatedClause = new CockroachDBNotOperation(whereClause);
		CockroachDBUnaryPostfixOperation notNullClause = new CockroachDBUnaryPostfixOperation(whereClause,
				CockroachDBUnaryPostfixOperator.IS_NULL);
		List<CockroachDBExpression> mappedAggregate = mapped(aggregate);
		CockroachDBSelect leftSelect = getSelect(mappedAggregate, from, whereClause, select.getJoinList());
		CockroachDBSelect middleSelect = getSelect(mappedAggregate, from, negatedClause, select.getJoinList());
		CockroachDBSelect rightSelect = getSelect(mappedAggregate, from, notNullClause, select.getJoinList());
		String metamorphicText = "SELECT " + getOuterAggregateFunction(aggregate).toString() + " FROM (";
		metamorphicText += CockroachDBVisitor.asString(leftSelect) + " UNION ALL "
				+ CockroachDBVisitor.asString(middleSelect) + " UNION ALL " + CockroachDBVisitor.asString(rightSelect);
		metamorphicText += ")";
		String firstResult;
		String secondResult;
		QueryAdapter q = new QueryAdapter(originalQuery, errors);
		state.getState().queryString = "--" + originalQuery + ";\n--" + metamorphicText + "\n-- ";
		try (ResultSet result = q.executeAndGet(state.getConnection())) {
			if (result == null) {
				throw new IgnoreMeException();
			}
			if (!result.next()) {
				throw new IgnoreMeException();
			}
			firstResult = result.getString(1);
		}

		QueryAdapter q2 = new QueryAdapter(metamorphicText, errors);
		try (ResultSet result = q2.executeAndGet(state.getConnection())) {
			if (result == null) {
				throw new IgnoreMeException();
			}
			result.next();
			secondResult = result.getString(1);
		} catch (PSQLException e) {
			throw new AssertionError(metamorphicText, e);
		}
		state.getState().queryString = "--" + originalQuery + ";\n--" + metamorphicText + "\n-- " + firstResult + "\n-- "
				+ secondResult;
		if (firstResult == null && secondResult != null
				|| firstResult != null && (!firstResult.contentEquals(secondResult)
						&& !DatabaseProvider.isEqualDouble(firstResult, secondResult))) {
			if (secondResult.contains("Inf")) {
				throw new IgnoreMeException(); // FIXME: average computation
			}
			throw new AssertionError();
		}

	}

	private List<CockroachDBExpression> mapped(CockroachDBAggregate aggregate) {
		switch (aggregate.getFunc()) {
		case SUM:
		case COUNT:
		case COUNT_ROWS:
		case BIT_AND:
		case BIT_OR:
		case XOR_AGG:
		case SUM_INT:
		case BOOL_AND:
		case BOOL_OR:
		case MAX:
		case MIN:
			return aliasArgs(Arrays.asList(aggregate));
		case AVG:
//			List<CockroachDBExpression> arg = Arrays.asList(new CockroachDBCast(aggregate.getExpr().get(0), CockroachDBDataType.DECIMAL.get()));
			CockroachDBAggregate sum = new CockroachDBAggregate(CockroachDBAggregateFunction.SUM, aggregate.getExpr());
			CockroachDBCast count = new CockroachDBCast(new CockroachDBAggregate(CockroachDBAggregateFunction.COUNT, aggregate.getExpr()), CockroachDBDataType.DECIMAL.get());
//			CockroachDBBinaryArithmeticOperation avg = new CockroachDBBinaryArithmeticOperation(sum, count, CockroachDBBinaryArithmeticOperator.DIV);
			return aliasArgs(Arrays.asList(sum, count));
		default:
			throw new AssertionError(aggregate.getFunc());
		}
	}

	private List<CockroachDBExpression> aliasArgs(List<CockroachDBExpression> originalAggregateArgs) {
		List<CockroachDBExpression> args = new ArrayList<>();
		int i = 0;
		for (CockroachDBExpression expr : originalAggregateArgs) {
			args.add(new CockroachDBAlias(expr, "agg" + i++));
		}
		return args;
	}

	private String getOuterAggregateFunction(CockroachDBAggregate aggregate) {
		switch (aggregate.getFunc()) {
		case AVG:
			return "SUM(agg0::DECIMAL)/SUM(agg1)::DECIMAL";
		case COUNT:
		case COUNT_ROWS:
			return CockroachDBAggregateFunction.SUM.toString() + "(agg0)";
		default:
			return aggregate.getFunc().toString() + "(agg0)";
		}
	}

	private CockroachDBSelect getSelect(List<CockroachDBExpression> aggregates, List<CockroachDBTableReference> from,
			CockroachDBExpression whereClause, List<CockroachDBJoin> joinList) {
		CockroachDBSelect leftSelect = new CockroachDBSelect();
		leftSelect.setColumns(aggregates);
		leftSelect.setFromTables(from);
		leftSelect.setWhereCondition(whereClause);
		leftSelect.setJoinList(joinList);
		if (Randomly.getBooleanWithSmallProbability()) {
			leftSelect.setGroupByClause(gen.generateExpressions(Randomly.smallNumber() + 1));
		}
		return leftSelect;
	}

}
