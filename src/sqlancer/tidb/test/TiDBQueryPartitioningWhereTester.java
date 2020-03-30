package sqlancer.tidb.test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
import sqlancer.tidb.TiDBSchema.TiDBTable;
import sqlancer.tidb.TiDBSchema.TiDBTables;
import sqlancer.tidb.ast.TiDBColumnReference;
import sqlancer.tidb.ast.TiDBExpression;
import sqlancer.tidb.ast.TiDBJoin;
import sqlancer.tidb.ast.TiDBSelect;
import sqlancer.tidb.ast.TiDBTableReference;
import sqlancer.tidb.ast.TiDBUnaryPostfixOperation;
import sqlancer.tidb.ast.TiDBUnaryPostfixOperation.TiDBUnaryPostfixOperator;
import sqlancer.tidb.ast.TiDBUnaryPrefixOperation;
import sqlancer.tidb.ast.TiDBUnaryPrefixOperation.TiDBUnaryPrefixOperator;
import sqlancer.tidb.visitor.TiDBVisitor;

public class TiDBQueryPartitioningWhereTester  implements TestOracle  {

	private final TiDBGlobalState state;
	private final Set<String> errors = new HashSet<>();

	public TiDBQueryPartitioningWhereTester(TiDBGlobalState state) {
		this.state = state;
		TiDBErrors.addExpressionErrors(errors);
	}

	@Override
	public void check() throws SQLException {
		TiDBSchema s = state.getSchema();
		TiDBTables targetTables = s.getRandomTableNonEmptyTables();
		TiDBExpressionGenerator gen = new TiDBExpressionGenerator(state).setColumns(targetTables.getColumns());
		TiDBSelect select = new TiDBSelect();
		select.setFetchColumns(Arrays.asList(new TiDBColumnReference(targetTables.getColumns().get(0))));
		List<TiDBTable> tables = targetTables.getTables();
		List<TiDBExpression> tableList = tables.stream()
				.map(t -> new TiDBTableReference(t)).collect(Collectors.toList());
		List<TiDBExpression> joinExpressions = TiDBJoin.getJoins(tableList, state);
		select.setFromList(tableList);
		select.setJoins(joinExpressions);
		select.setWhereClause(null);
		if (Randomly.getBoolean() && false) {
			// TODO: this results in run-time errors
			select.setOrderByExpressions(gen.generateOrderBys());
		}
		String originalQueryString = TiDBVisitor.asString(select);
		
		List<String> resultSet = DatabaseProvider.getResultSetFirstColumnAsString(originalQueryString, errors, state.getConnection());
		
		TiDBExpression predicate = gen.generateExpression();
		select.setOrderByExpressions(Collections.emptyList());
		select.setWhereClause(predicate);
		String firstQueryString = TiDBVisitor.asString(select);
		select.setWhereClause(new TiDBUnaryPrefixOperation(predicate, TiDBUnaryPrefixOperator.NOT));
		String secondQueryString = TiDBVisitor.asString(select);
		select.setWhereClause(new TiDBUnaryPostfixOperation(predicate, TiDBUnaryPostfixOperator.IS_NULL));
		String thirdQueryString = TiDBVisitor.asString(select);
		List<String> secondResultSet;
		String combinedString = firstQueryString + " UNION ALL " + secondQueryString + " UNION ALL " + thirdQueryString;
		if (Randomly.getBoolean()) {
			secondResultSet = DatabaseProvider.getResultSetFirstColumnAsString(combinedString, errors, state.getConnection());
		} else {
			secondResultSet = new ArrayList<>();
			secondResultSet.addAll(DatabaseProvider.getResultSetFirstColumnAsString(firstQueryString, errors, state.getConnection()));
			secondResultSet.addAll(DatabaseProvider.getResultSetFirstColumnAsString(secondQueryString, errors, state.getConnection()));
			secondResultSet.addAll(DatabaseProvider.getResultSetFirstColumnAsString(thirdQueryString, errors, state.getConnection()));
		}
		if (state.getOptions().logEachSelect()) {
			state.getLogger().writeCurrent(originalQueryString);
			state.getLogger().writeCurrent(combinedString);
		}
		if (resultSet.size() != secondResultSet.size()) {
			throw new AssertionError(originalQueryString + ";\n" + combinedString + ";");
		}
	}
}