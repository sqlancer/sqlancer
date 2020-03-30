package sqlancer.cockroachdb.test;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import sqlancer.DatabaseProvider;
import sqlancer.Randomly;
import sqlancer.TestOracle;
import sqlancer.cockroachdb.CockroachDBCommon;
import sqlancer.cockroachdb.CockroachDBErrors;
import sqlancer.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;
import sqlancer.cockroachdb.CockroachDBSchema;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBDataType;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBTables;
import sqlancer.cockroachdb.CockroachDBVisitor;
import sqlancer.cockroachdb.ast.CockroachDBColumnReference;
import sqlancer.cockroachdb.ast.CockroachDBExpression;
import sqlancer.cockroachdb.ast.CockroachDBNotOperation;
import sqlancer.cockroachdb.ast.CockroachDBSelect;
import sqlancer.cockroachdb.ast.CockroachDBTableReference;
import sqlancer.cockroachdb.ast.CockroachDBUnaryPostfixOperation;
import sqlancer.cockroachdb.ast.CockroachDBUnaryPostfixOperation.CockroachDBUnaryPostfixOperator;
import sqlancer.cockroachdb.gen.CockroachDBExpressionGenerator;

public class CockroachDBQueryPartitioningWhereTester implements TestOracle {

	private final CockroachDBGlobalState state;
	private final Set<String> errors = new HashSet<>();

	public CockroachDBQueryPartitioningWhereTester(CockroachDBGlobalState state) {
		this.state = state;
		CockroachDBErrors.addExpressionErrors(errors);
		errors.add("GROUP BY term out of range");
	}

	@Override
	public void check() throws SQLException {
		CockroachDBSchema s = state.getSchema();
		CockroachDBTables targetTables = s.getRandomTableNonEmptyTables();
		CockroachDBExpressionGenerator gen = new CockroachDBExpressionGenerator(state)
				.setColumns(targetTables.getColumns());
		CockroachDBSelect select = new CockroachDBSelect();
		select.setFetchColumns(Arrays.asList(new CockroachDBColumnReference(targetTables.getColumns().get(0))));
		List<CockroachDBTableReference> tableList = targetTables.getTables().stream()
				.map(t -> new CockroachDBTableReference(t)).collect(Collectors.toList());
		List<CockroachDBExpression> from = CockroachDBCommon.getTableReferences(tableList);
		if (Randomly.getBooleanWithRatherLowProbability()) {
			select.setJoinList(CockroachDBNoRECTester.getJoins(from, state));
		}
		select.setFromList(from);
		// TODO order by?
		select.setWhereClause(null);
		String originalQueryString = CockroachDBVisitor.asString(select);

		List<String> resultSet = DatabaseProvider.getResultSetFirstColumnAsString(originalQueryString, errors,
				state.getConnection(), state);

		CockroachDBExpression predicate = gen.generateExpression(CockroachDBDataType.BOOL.get());
		select.setWhereClause(predicate);
		String firstQueryString = CockroachDBVisitor.asString(select);
		select.setWhereClause(new CockroachDBNotOperation(predicate));
		String secondQueryString = CockroachDBVisitor.asString(select);
		select.setWhereClause(new CockroachDBUnaryPostfixOperation(predicate, CockroachDBUnaryPostfixOperator.IS_NULL));
		String thirdQueryString = CockroachDBVisitor.asString(select);
		String combinedString = firstQueryString + " UNION ALL " + secondQueryString + " UNION ALL " + thirdQueryString;
		List<String> secondResultSet = DatabaseProvider.getResultSetFirstColumnAsString(combinedString, errors,
				state.getConnection(), state);
		if (state.getOptions().logEachSelect()) {
			state.getLogger().writeCurrent(originalQueryString);
			state.getLogger().writeCurrent(combinedString);
		}
		TestOracle.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQueryString, combinedString, state);
	}
}
