package sqlancer.tidb.test;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

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
import sqlancer.tidb.gen.TiDBHintGenerator;

public abstract class TiDBQueryPartitioningBase implements TestOracle {

	final TiDBGlobalState state;
	final Set<String> errors = new HashSet<>();

	TiDBSchema s;
	TiDBTables targetTables;
	TiDBExpressionGenerator gen;
	TiDBSelect select;
	TiDBExpression predicate;
	TiDBExpression negatedPredicate;
	TiDBExpression isNullPredicate;

	public TiDBQueryPartitioningBase(TiDBGlobalState state) {
		this.state = state;
		TiDBErrors.addExpressionErrors(errors);
	}

	@Override
	public void check() throws SQLException {
		s = state.getSchema();
		targetTables = s.getRandomTableNonEmptyTables();
		gen = new TiDBExpressionGenerator(state).setColumns(targetTables.getColumns());
		select = new TiDBSelect();
		select.setFetchColumns(generateFetchColumns());
		List<TiDBTable> tables = targetTables.getTables();
		if (Randomly.getBoolean()) {
			TiDBHintGenerator.generateHints(select, tables);
		}

		List<TiDBExpression> tableList = tables.stream().map(t -> new TiDBTableReference(t))
				.collect(Collectors.toList());
		List<TiDBExpression> joins = TiDBJoin.getJoins(tableList, state);
		select.setJoinList(joins);
		select.setFromList(tableList);
		select.setWhereClause(null);
		predicate = generatePredicate();
		negatedPredicate = new TiDBUnaryPrefixOperation(predicate, TiDBUnaryPrefixOperator.NOT);
		isNullPredicate = new TiDBUnaryPostfixOperation(predicate, TiDBUnaryPostfixOperator.IS_NULL);
	}

	List<TiDBExpression> generateFetchColumns() {
		return Arrays.asList(new TiDBColumnReference(targetTables.getColumns().get(0)));
	}

	TiDBExpression generatePredicate() {
		return gen.generateExpression();
	}

}
