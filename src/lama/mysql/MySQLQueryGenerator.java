package lama.mysql;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import lama.Main;
import lama.Main.QueryManager;
import lama.Main.StateLogger;
import lama.MainOptions;
import lama.Randomly;
import lama.StateToReproduce;
import lama.StateToReproduce.MySQLStateToReproduce;
import lama.mysql.MySQLSchema.MySQLColumn;
import lama.mysql.MySQLSchema.MySQLRowValue;
import lama.mysql.MySQLSchema.MySQLTable;
import lama.mysql.MySQLSchema.MySQLTables;
import lama.mysql.ast.MySQLColumnValue;
import lama.mysql.ast.MySQLConstant;
import lama.mysql.ast.MySQLExpression;
import lama.mysql.ast.MySQLOrderByTerm;
import lama.mysql.ast.MySQLOrderByTerm.MySQLOrder;
import lama.mysql.ast.MySQLSelect;
import lama.mysql.ast.MySQLUnaryPostfixOperator;
import lama.mysql.ast.MySQLUnaryPostfixOperator.UnaryPostfixOperator;
import lama.mysql.ast.MySQLUnaryPrefixOperation;
import lama.mysql.ast.MySQLUnaryPrefixOperation.MySQLUnaryPrefixOperator;
import lama.mysql.gen.MySQLRandomExpressionGenerator;

public class MySQLQueryGenerator {

	private QueryManager manager;
	private Randomly r;
	private Connection database;
	private StateToReproduce state;
	private MySQLSchema s;
	private MySQLRowValue rw;
	private List<MySQLColumn> fetchColumns;

	public MySQLQueryGenerator(QueryManager manager, Randomly r, Connection con, String databaseName)
			throws SQLException {
		this.manager = manager;
		this.r = r;
		this.database = con;
		this.s = MySQLSchema.fromConnection(con, databaseName);
	}

	public void generateAndCheckQuery(MySQLStateToReproduce state, StateLogger logger, MainOptions options)
			throws SQLException {
		String queryString = getQueryThatContainsAtLeastOneRow(state);

		try {
			boolean isContainedIn = isContainedIn(queryString, options, logger);
			if (!isContainedIn) {
				throw new Main.ReduceMeException();
			}
		} catch (SQLException e) {
			if (e.getMessage().contains("BIGINT value is out of range")) {
				// IGNORE
			} else {
				throw e;
			}
		}
	}

	public String getQueryThatContainsAtLeastOneRow(MySQLStateToReproduce state) throws SQLException {
		this.state = state;
		MySQLTables randomFromTables = s.getRandomTableNonEmptyTables();
		List<MySQLTable> tables = randomFromTables.getTables();

		state.queryTargetedTablesString = randomFromTables.tableNamesAsString();

		MySQLSelect selectStatement = new MySQLSelect();
		selectStatement.setSelectType(Randomly.fromOptions(MySQLSelect.SelectType.values()));
		state.whereClause = selectStatement;
		List<MySQLColumn> columns = randomFromTables.getColumns();
//		for (MySQLTable t : tables) {
//			if (t.getRowid() != null) {
//				columns.add(t.getRowid());
//			}
//		}
		rw = randomFromTables.getRandomRowValue(database, state);

//		List<Join> joinStatements = new ArrayList<>();
//		for (int i = 1; i < tables.size(); i++) {
//			SQLite3Expression joinClause = generateWhereClauseThatContainsRowValue(columns, rw);
//			Table table = Randomly.fromList(tables);
//			tables.remove(table);
//			JoinType options;
//			if (tables.size() == 2) {
//				// allow outer with arbitrary column order (see error: ON clause references
//				// tables to its right)
//				options = Randomly.fromOptions(JoinType.INNER, JoinType.CROSS, JoinType.OUTER);
//			} else {
//				options = Randomly.fromOptions(JoinType.INNER, JoinType.CROSS);
//			}
//			Join j = new SQLite3Expression.Join(table, joinClause, options);
//			joinStatements.add(j);
//		}
//		selectStatement.setJoinClauses(joinStatements);
		selectStatement.setFromTables(tables);

		fetchColumns = columns;
		selectStatement.selectFetchColumns(fetchColumns);
		state.queryTargetedColumnsString = fetchColumns.stream().map(c -> c.getFullQualifiedName())
				.collect(Collectors.joining(", "));
		MySQLExpression whereClause = generateWhereClauseThatContainsRowValue(columns, rw);
		selectStatement.setWhereClause(whereClause);
		state.whereClause = selectStatement;
		List<MySQLExpression> groupByClause = generateGroupByClause(columns, rw);
		selectStatement.setGroupByClause(groupByClause);
		MySQLExpression limitClause = generateLimit();
		selectStatement.setLimitClause(limitClause);
		if (limitClause != null) {
			MySQLExpression offsetClause = generateOffset();
			selectStatement.setOffsetClause(offsetClause);
		}
		List<String> modifiers = Randomly.subset("STRAIGHT_JOIN", "SQL_SMALL_RESULT", "SQL_BIG_RESULT", "SQL_NO_CACHE"); // "SQL_BUFFER_RESULT",
																															// "SQL_CALC_FOUND_ROWS",
																															// "HIGH_PRIORITY"
		// TODO: Incorrect usage/placement of 'SQL_BUFFER_RESULT'
		selectStatement.setModifiers(modifiers);
		List<MySQLExpression> orderBy = generateOrderBy(columns);
		selectStatement.setOrderByClause(orderBy);

		StringBuilder sb2 = new StringBuilder();
		sb2.append("SELECT * FROM (SELECT * FROM ");
		sb2.append(randomFromTables.tableNamesAsString());
		sb2.append(" WHERE ");
		int i = 0;
		for (MySQLColumn c : fetchColumns) {
			if (i++ != 0) {
				sb2.append(" AND ");
			}
			sb2.append(c.getFullQualifiedName());
			if (rw.getValues().get(c).isNull()) {
				sb2.append(" IS NULL");
			} else {
				sb2.append(" = ");
				sb2.append(rw.getValues().get(c).getTextRepresentation());
			}
		}
		sb2.append(") as result;");
		state.queryThatSelectsRow = sb2.toString();

		MySQLToStringVisitor visitor = new MySQLToStringVisitor();
		visitor.visit(selectStatement);
		String queryString = visitor.get();
		return queryString;
	}

	private List<MySQLExpression> generateGroupByClause(List<MySQLColumn> columns, MySQLRowValue rw) {
		if (Randomly.getBoolean()) {
			return columns.stream().map(c -> MySQLColumnValue.create(c, rw.getValues().get(c)))
					.collect(Collectors.toList());
		} else {
			return Collections.emptyList();
		}
	}

	public List<MySQLExpression> generateOrderBy(List<MySQLColumn> columns) {
		List<MySQLExpression> orderBys = new ArrayList<>();
		for (int i = 0; i < Randomly.smallNumber(); i++) {
			orderBys.add(new MySQLOrderByTerm(MySQLColumnValue.create(Randomly.fromList(columns), null),
					MySQLOrder.getRandomOrder()));
		}
		return orderBys;
	}

	private MySQLConstant generateLimit() {
		if (Randomly.getBoolean()) {
			return MySQLConstant.createIntConstant(Integer.MAX_VALUE);
		} else {
			return null;
		}
	}

	private MySQLExpression generateOffset() {
		if (Randomly.getBoolean()) {
			// OFFSET 0
			return MySQLConstant.createIntConstantNotAsBoolean(0);
		} else {
			return null;
		}
	}

	private MySQLExpression generateWhereClauseThatContainsRowValue(List<MySQLColumn> columns, MySQLRowValue rw) {
		MySQLExpression expression = MySQLRandomExpressionGenerator.generateRandomExpression(columns, rw, r);
		MySQLConstant expectedValue = expression.getExpectedValue();
		if (expectedValue.isNull()) {
			return new MySQLUnaryPostfixOperator(expression, UnaryPostfixOperator.IS_NULL, false);
		} else if (expectedValue.asBooleanNotNull()) {
			return expression;
		} else {
			return new MySQLUnaryPrefixOperation(expression, MySQLUnaryPrefixOperator.NOT);
		}
	}

	private boolean isContainedIn(String queryString, MainOptions options, StateLogger logger) throws SQLException {
		Statement createStatement;
		createStatement = database.createStatement();

		StringBuilder sb = new StringBuilder();
		sb.append("SELECT * FROM ("); // ANOTHER SELECT TO USE ORDER BY without restrictions
		sb.append(queryString);
		sb.append(") as result WHERE ");
		int i = 0;
		for (MySQLColumn c : fetchColumns) {
			if (i++ != 0) {
				sb.append(" AND ");
			}
			sb.append("result." + c.getTable().getName() + c.getName());
			if (rw.getValues().get(c).isNull()) {
				sb.append(" IS NULL");
			} else {
				sb.append(" = ");
				sb.append(rw.getValues().get(c).getTextRepresentation());
			}
		}

		String resultingQueryString = sb.toString();
		state.queryString = resultingQueryString;
		if (options.logEachSelect()) {
			logger.writeCurrent(resultingQueryString);
		}
		try (ResultSet result = createStatement.executeQuery(resultingQueryString)) {
			boolean isContainedIn = result.next();
			createStatement.close();
			return isContainedIn;
		}
	}
}
