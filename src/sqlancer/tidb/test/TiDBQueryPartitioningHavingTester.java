package sqlancer.tidb.test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import sqlancer.DatabaseProvider;
import sqlancer.Randomly;
import sqlancer.TestOracle;
import sqlancer.tidb.TiDBErrors;
import sqlancer.tidb.TiDBExpressionGenerator;
import sqlancer.tidb.TiDBProvider.TiDBGlobalState;
import sqlancer.tidb.TiDBSchema;
import sqlancer.tidb.TiDBSchema.TiDBTables;
import sqlancer.tidb.ast.TiDBColumnReference;
import sqlancer.tidb.ast.TiDBExpression;
import sqlancer.tidb.ast.TiDBSelect;
import sqlancer.tidb.ast.TiDBTableReference;
import sqlancer.tidb.ast.TiDBUnaryPostfixOperation;
import sqlancer.tidb.ast.TiDBUnaryPostfixOperation.TiDBUnaryPostfixOperator;
import sqlancer.tidb.ast.TiDBUnaryPrefixOperation;
import sqlancer.tidb.ast.TiDBUnaryPrefixOperation.TiDBUnaryPrefixOperator;
import sqlancer.tidb.visitor.TiDBVisitor;

public class TiDBQueryPartitioningHavingTester  implements TestOracle  {

	private final TiDBGlobalState state;
	private final Set<String> errors = new HashSet<>();

	public TiDBQueryPartitioningHavingTester(TiDBGlobalState state) {
		this.state = state;
		TiDBErrors.addExpressionErrors(errors);
		TiDBErrors.addExpressionHavingErrors(errors);
	}

	@Override
	public void check() throws SQLException {
		TiDBSchema s = state.getSchema();
		TiDBTables targetTables = s.getRandomTableNonEmptyTables();
		TiDBExpressionGenerator gen = new TiDBExpressionGenerator(state).setColumns(targetTables.getColumns());
		TiDBSelect select = new TiDBSelect();
		select.setFetchColumns(Arrays.asList(new TiDBColumnReference(targetTables.getColumns().get(0))));
		List<TiDBExpression> tableList = targetTables.getTables().stream()
				.map(t -> new TiDBTableReference(t)).collect(Collectors.toList());
//		List<TiDBTableReference> from = TiDBCommon.getTableReferences(tableList);
//		if (Randomly.getBooleanWithRatherLowProbability()) {
//			select.setJoinList(TiDBNoRECTester.getJoins(from, state));
//		}
		select.setFromList(tableList);
		// TODO order by?
		if (Randomly.getBoolean()) {
			select.setWhereClause(gen.generateExpression());
		}
		select.setGroupByExpressions(gen.generateExpressions(Randomly.smallNumber() + 1));
		select.setHavingClause(null);
		String originalQueryString = TiDBVisitor.asString(select);
		if (state.getOptions().logEachSelect()) {
			state.getLogger().writeCurrent(originalQueryString);
			try {
				state.getLogger().getCurrentFileWriter().flush();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		List<String> resultSet = DatabaseProvider.getResultSetFirstColumnAsString(originalQueryString, errors, state.getConnection());
		
		TiDBExpression predicate = gen.generateHavingClause();
		select.setHavingClause(predicate);
		String firstQueryString = TiDBVisitor.asString(select);
		select.setHavingClause(new TiDBUnaryPrefixOperation(predicate, TiDBUnaryPrefixOperator.NOT));
		String secondQueryString = TiDBVisitor.asString(select);
		select.setHavingClause(new TiDBUnaryPostfixOperation(predicate, TiDBUnaryPostfixOperator.IS_NULL));
		String thirdQueryString = TiDBVisitor.asString(select);
		String combinedString = firstQueryString + " UNION ALL " + secondQueryString + " UNION ALL " + thirdQueryString;
		if (state.getOptions().logEachSelect()) {
			state.getLogger().writeCurrent(combinedString);
			try {
				state.getLogger().getCurrentFileWriter().flush();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		List<String> secondResultSet = DatabaseProvider.getResultSetFirstColumnAsString(combinedString, errors, state.getConnection());
		if (resultSet.size() != secondResultSet.size()) {
			throw new AssertionError(originalQueryString + ";\n" + combinedString + ";");
		}
	}
}
