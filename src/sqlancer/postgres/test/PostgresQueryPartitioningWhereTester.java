package sqlancer.postgres.test;

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
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresSchema;
import sqlancer.postgres.PostgresSchema.PostgresDataType;
import sqlancer.postgres.PostgresSchema.PostgresTables;
import sqlancer.postgres.PostgresVisitor;
import sqlancer.postgres.ast.PostgresColumnValue;
import sqlancer.postgres.ast.PostgresExpression;
import sqlancer.postgres.ast.PostgresPostfixOperation;
import sqlancer.postgres.ast.PostgresPostfixOperation.PostfixOperator;
import sqlancer.postgres.ast.PostgresPrefixOperation;
import sqlancer.postgres.ast.PostgresSelect;
import sqlancer.postgres.ast.PostgresSelect.PostgresFromTable;
import sqlancer.postgres.gen.PostgresCommon;
import sqlancer.postgres.gen.PostgresExpressionGenerator;

public class PostgresQueryPartitioningWhereTester implements TestOracle {

		private final PostgresGlobalState state;
		private final Set<String> errors = new HashSet<>();

		public PostgresQueryPartitioningWhereTester(PostgresGlobalState state) {
			this.state = state;
			PostgresCommon.addCommonExpressionErrors(errors);
			PostgresCommon.addCommonFetchErrors(errors);
		}

		@Override
		public void check() throws SQLException {
			PostgresSchema s = state.getSchema();
			PostgresTables targetTables = s.getRandomTableNonEmptyTables();
			PostgresExpressionGenerator gen = new PostgresExpressionGenerator(state.getRandomly()).setColumns(targetTables.getColumns());
			PostgresSelect select = new PostgresSelect();
			select.setFetchColumns(Arrays.asList(new PostgresColumnValue(targetTables.getColumns().get(0), null)));
			List<PostgresFromTable> tableList = targetTables.getTables().stream()
					.map(t -> new PostgresFromTable(t, Randomly.getBoolean())).collect(Collectors.toList());
			// TODO joins
			select.setFromList(tableList);
			select.setWhereClause(null);
			if (Randomly.getBoolean() && false /* TODO */) {
				select.setOrderByClause(gen.generateOrderBy());
			}
			String originalQueryString = PostgresVisitor.asString(select);
			
			List<String> resultSet = DatabaseProvider.getResultSetFirstColumnAsString(originalQueryString, errors, state.getConnection());
			
			PostgresExpression predicate = gen.generateExpression(PostgresDataType.BOOLEAN);
			select.setOrderByClause(Collections.emptyList());
			select.setWhereClause(predicate);
			String firstQueryString = PostgresVisitor.asString(select);
			select.setWhereClause(new PostgresPrefixOperation(predicate, PostgresPrefixOperation.PrefixOperator.NOT));
			String secondQueryString = PostgresVisitor.asString(select);
			select.setWhereClause(new PostgresPostfixOperation(predicate, PostfixOperator.IS_NULL));
			String thirdQueryString = PostgresVisitor.asString(select);
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