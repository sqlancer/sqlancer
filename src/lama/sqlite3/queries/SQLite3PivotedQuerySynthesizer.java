package lama.sqlite3.queries;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import lama.Main;
import lama.Main.StateLogger;
import lama.MainOptions;
import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import lama.StateToReproduce.SQLite3StateToReproduce;
import lama.sqlite3.SQLite3Provider;
import lama.sqlite3.SQLite3ToStringVisitor;
import lama.sqlite3.SQLite3Visitor;
import lama.sqlite3.ast.SQLite3Aggregate;
import lama.sqlite3.ast.SQLite3Cast;
import lama.sqlite3.ast.SQLite3Aggregate.SQLite3AggregateFunction;
import lama.sqlite3.ast.SQLite3Constant;
import lama.sqlite3.ast.SQLite3Expression;
import lama.sqlite3.ast.SQLite3Expression.ColumnName;
import lama.sqlite3.ast.SQLite3Expression.Join;
import lama.sqlite3.ast.SQLite3Expression.Join.JoinType;
import lama.sqlite3.ast.SQLite3Expression.SQLite3OrderingTerm;
import lama.sqlite3.ast.SQLite3Expression.SQLite3OrderingTerm.Ordering;
import lama.sqlite3.ast.SQLite3Expression.PostfixUnaryOperation;
import lama.sqlite3.ast.SQLite3Expression.PostfixUnaryOperation.PostfixUnaryOperator;
import lama.sqlite3.ast.SQLite3Expression.SQLite3Distinct;
import lama.sqlite3.ast.SQLite3Expression.SQLite3PostfixText;
import lama.sqlite3.ast.SQLite3SelectStatement;
import lama.sqlite3.ast.SQLite3WindowFunction;
import lama.sqlite3.ast.SQLite3UnaryOperation;
import lama.sqlite3.ast.SQLite3UnaryOperation.UnaryOperator;
import lama.sqlite3.gen.SQLite3Common;
import lama.sqlite3.gen.SQLite3ExpressionGenerator;
import lama.sqlite3.schema.SQLite3Schema;
import lama.sqlite3.schema.SQLite3Schema.Column;
import lama.sqlite3.schema.SQLite3Schema.RowValue;
import lama.sqlite3.schema.SQLite3Schema.Table;
import lama.sqlite3.schema.SQLite3Schema.Tables;

public class SQLite3PivotedQuerySynthesizer {

	private final Connection database;
	private final SQLite3Schema s;
	private final Randomly r;
	private SQLite3StateToReproduce state;
	private RowValue rw;
	private List<Column> fetchColumns;
	private final List<String> errors = new ArrayList<>();
	private List<SQLite3Expression> colExpressions;

	public SQLite3PivotedQuerySynthesizer(Connection con, Randomly r) throws SQLException {
		this.database = con;
		this.r = r;
		s = SQLite3Schema.fromConnection(database);
	}

	public void generateAndCheckQuery(SQLite3StateToReproduce state, StateLogger logger, MainOptions options)
			throws SQLException {
		Query query = getQueryThatContainsAtLeastOneRow(state);
		if (options.logEachSelect()) {
			logger.writeCurrent(query.getQueryString());
		}
		boolean isContainedIn = isContainedIn(query);
		if (!isContainedIn) {
			throw new Main.ReduceMeException();
		}
	}

	public Query getQueryThatContainsAtLeastOneRow(SQLite3StateToReproduce state) throws SQLException {
		SQLite3SelectStatement selectStatement = getQuery(state);
		SQLite3ToStringVisitor visitor = new SQLite3ToStringVisitor();
		visitor.visit(selectStatement);
		String queryString = visitor.get();
		addExpectedErrors(errors);
		return new QueryAdapter(queryString, errors);
	}

	public static void addExpectedErrors(List<String> errors) {
		errors.add("no such index");
		errors.add("no query solution");
		errors.add(
				"[SQLITE_ERROR] SQL error or missing database (second argument to likelihood() must be a constant between 0.0 and 1.0)");
		errors.add("[SQLITE_ERROR] SQL error or missing database (integer overflow)");
		errors.add("[SQLITE_ERROR] SQL error or missing database (parser stack overflow)");
		errors.add("second argument to nth_value must be a positive integer");
		errors.add("misuse of aggregate");
		errors.add("GROUP BY term out of range");
	}

	public SQLite3SelectStatement getQuery(SQLite3StateToReproduce state) throws SQLException {
		this.state = state;
		Tables randomFromTables = s.getRandomTableNonEmptyTables();
		List<Table> tables = randomFromTables.getTables();

		state.queryTargetedTablesString = randomFromTables.tableNamesAsString();
		SQLite3SelectStatement selectStatement = new SQLite3SelectStatement();
		selectStatement.setSelectType(Randomly.fromOptions(SQLite3SelectStatement.SelectType.values()));
		List<Column> columns = randomFromTables.getColumns();
		for (Table t : tables) {
			if (t.getRowid() != null) {
				columns.add(t.getRowid());
			}
		}
		rw = randomFromTables.getRandomRowValue(database, state);

		List<Join> joinStatements = new ArrayList<>();
		for (int i = 1; i < tables.size(); i++) {
			SQLite3Expression joinClause = generateWhereClauseThatContainsRowValue(columns, rw);
			Table table = Randomly.fromList(tables);
			tables.remove(table);
			JoinType options;
			options = Randomly.fromOptions(JoinType.INNER, JoinType.CROSS, JoinType.OUTER);
			if (options == JoinType.OUTER && tables.size() > 2) {
				errors.add("ON clause references tables to its right");
			}
			Join j = new SQLite3Expression.Join(table, joinClause, options);
			joinStatements.add(j);
		}
		selectStatement.setJoinClauses(joinStatements);
		selectStatement.setFromTables(tables);

		// TODO: also implement a wild-card check (*)
		// filter out row ids from the select because the hinder the reduction process
		// once a bug is found
		List<Column> columnsWithoutRowid = columns.stream().filter(c -> !c.getName().matches("rowid"))
				.collect(Collectors.toList());
		fetchColumns = Randomly.nonEmptySubset(columnsWithoutRowid);
		colExpressions = new ArrayList<>();
		List<Table> allTables = new ArrayList<>();
		allTables.addAll(tables);
		allTables.addAll(joinStatements.stream().map(join -> join.getTable()).collect(Collectors.toList()));
		boolean allTablesContainOneRow = allTables.stream().allMatch(t -> t.getNrRows() == 1);
		for (Column c : fetchColumns) {
			SQLite3Expression colName = new ColumnName(c, rw.getValues().get(c));
			if (allTablesContainOneRow && Randomly.getBoolean()) {
				boolean generateDistinct = Randomly.getBoolean();
				if (generateDistinct) {
					colName = new SQLite3Distinct(colName);
				}
				SQLite3AggregateFunction aggFunc = SQLite3AggregateFunction.getRandom(c.getColumnType());
				colName = new SQLite3Aggregate(colName, aggFunc);
				if (Randomly.getBoolean() && !generateDistinct) {
					colName = generateWindowFunction(columns, colName, true);
				}
				errors.add("second argument to nth_value must be a positive integer");
			}
			if (Randomly.getBoolean()) {
				SQLite3Expression randomExpression;
				do {
					randomExpression = new SQLite3ExpressionGenerator(r).setColumns(columns).getRandomExpression();
				} while (randomExpression.getExpectedValue() == null);
				colExpressions.add(randomExpression);
			} else {
				colExpressions.add(colName);
			}
		}
		if (Randomly.getBoolean() && allTablesContainOneRow) {
			SQLite3WindowFunction windowFunction = SQLite3WindowFunction.getRandom(columnsWithoutRowid, r);
			SQLite3Expression windowExpr = generateWindowFunction(columnsWithoutRowid, windowFunction, false);
			colExpressions.add(windowExpr);
		}
		selectStatement.setFetchColumns(colExpressions);
		state.queryTargetedColumnsString = fetchColumns.stream().map(c -> c.getFullQualifiedName())
				.collect(Collectors.joining(", "));
		SQLite3Expression whereClause = generateWhereClauseThatContainsRowValue(columns, rw);
		selectStatement.setWhereClause(whereClause);
		state.whereClause = selectStatement;
		List<SQLite3Expression> groupByClause = generateGroupByClause(columns, rw, allTablesContainOneRow);
		selectStatement.setGroupByClause(groupByClause);
		SQLite3Expression limitClause = generateLimit((long) (Math.pow(SQLite3Provider.NR_INSERT_ROW_TRIES, joinStatements.size() + randomFromTables.getTables().size())));
		selectStatement.setLimitClause(limitClause);
		if (limitClause != null) {
			SQLite3Expression offsetClause = generateOffset();
			selectStatement.setOffsetClause(offsetClause);
		}
		List<SQLite3Expression> orderBy = generateOrderBy(columns);
		selectStatement.setOrderByClause(orderBy);
		if (groupByClause.size() != 0 && Randomly.getBoolean()) {
			SQLite3Expression randomExpression = SQLite3Common.getTrueExpression(columns, r);
			if (Randomly.getBoolean()) {
				SQLite3AggregateFunction aggFunc = SQLite3AggregateFunction.getRandom();
				randomExpression = new SQLite3Aggregate(randomExpression, aggFunc);
			}
			selectStatement.setHavingClause(randomExpression);
		}
		return selectStatement;
	}

	private SQLite3Expression generateOffset() {
		if (Randomly.getBoolean()) {
			// OFFSET 0
			return SQLite3Constant.createIntConstant(0);
		} else {
			return null;
		}
	}

	public static boolean shouldIgnoreException(SQLException e) {
		return e.getMessage().contentEquals("[SQLITE_ERROR] SQL error or missing database (integer overflow)")
				|| e.getMessage().startsWith("[SQLITE_ERROR] SQL error or missing database (parser stack overflow)")
				|| e.getMessage().startsWith(
						"[SQLITE_ERROR] SQL error or missing database (second argument to likelihood() must be a constant between 0.0 and 1.0)")
				|| e.getMessage().contains("second argument to nth_value must be a positive integer");
	}

	private boolean isContainedIn(Query query) throws SQLException {
		Statement createStatement;
		createStatement = database.createStatement();

		StringBuilder sb = new StringBuilder();
		sb.append("SELECT ");
		addExpectedValues(sb);
		StringBuilder sb2 = new StringBuilder();
		addExpectedValues(sb2);
		state.values = sb2.toString();
		sb.append(" INTERSECT SELECT * FROM ("); // ANOTHER SELECT TO USE ORDER BY without restrictions
		sb.append(query.getQueryString());
		sb.append(")");
		String resultingQueryString = sb.toString();
		state.queryString = resultingQueryString;
		Query finalQuery = new QueryAdapter(resultingQueryString, query.getExpectedErrors());
		try (ResultSet result = createStatement.executeQuery(finalQuery.getQueryString())) {
			boolean isContainedIn = !result.isClosed();
			createStatement.close();
			return isContainedIn;
		} catch (SQLException e) {
			for (String exp : finalQuery.getExpectedErrors()) {
				if (e.getMessage().contains(exp)) {
					return true;
				}
			}
			throw e;
		}
	}

	private void addExpectedValues(StringBuilder sb) {
		for (int i = 0; i < colExpressions.size(); i++) {
			if (i != 0) {
				sb.append(", ");
			}
			SQLite3Constant expectedValue = colExpressions.get(i).getExpectedValue();
			sb.append(SQLite3Visitor.asString(expectedValue));
		}
	}

	public List<SQLite3Expression> generateOrderBy(List<Column> columns) {
		List<SQLite3Expression> orderBys = new ArrayList<>();
		for (int i = 0; i < Randomly.smallNumber(); i++) {
			SQLite3Expression expr;
			expr = new SQLite3ExpressionGenerator(r).setCon(database).setState(state).setColumns(columns).getRandomExpression();
			Ordering order = Randomly.fromOptions(Ordering.ASC, Ordering.DESC);
			orderBys.add(new SQLite3OrderingTerm(expr, order));
			// TODO RANDOM()
		}
		// TODO collate
		errors.add("ORDER BY term out of range");
		return orderBys;
	}

	private SQLite3Expression generateLimit(long l) {
		if (Randomly.getBoolean()) {
			return SQLite3Constant.createIntConstant(r.getLong(l, Long.MAX_VALUE));
		} else {
			return null;
		}
	}

	private List<SQLite3Expression> generateGroupByClause(List<Column> columns, RowValue rw, boolean allTablesContainOneRow) {
		errors.add("GROUP BY term out of range");
		if (allTablesContainOneRow && Randomly.getBoolean()) {
			List<SQLite3Expression> collect = new ArrayList<>();
			for (int i = 0; i < Randomly.smallNumber(); i++) {
				collect.add(new SQLite3ExpressionGenerator(r).setCon(database).setState((SQLite3StateToReproduce) state).setColumns(columns).setRowValue(rw).getRandomExpression());
			}
			return collect;
		}
		if (Randomly.getBoolean()) {
			// ensure that we GROUP BY all columns
			List<SQLite3Expression> collect = columns.stream().map(c -> new ColumnName(c, rw.getValues().get(c))).collect(Collectors.toList());
			if (Randomly.getBoolean()) {
				for (int i = 0; i < Randomly.smallNumber(); i++) {
					collect.add(new SQLite3ExpressionGenerator(r).setCon(database).setState((SQLite3StateToReproduce) state).setColumns(columns).setRowValue(rw).getRandomExpression());
				}
			}
			return collect;
		} else {
			return Collections.emptyList();
		}
	}

	private SQLite3Expression generateWhereClauseThatContainsRowValue(List<Column> columns, RowValue rw) {

		SQLite3Expression whereClause = generateNewExpression(columns, rw, true, 0);

		return whereClause;
	}

	private SQLite3Expression generateNewExpression(List<Column> columns, RowValue rw, boolean shouldBeTrue,
			int depth) {
		do {
			SQLite3Expression expr = new SQLite3ExpressionGenerator(r).setRowValue(rw).setState(state).setColumns(columns).setRowValue(rw).getRandomExpression();
			if (expr.getExpectedValue() != null) {
				if (expr.getExpectedValue().isNull()) {
					return new PostfixUnaryOperation(PostfixUnaryOperator.ISNULL, expr);
				}
				if (SQLite3Cast.isTrue(expr.getExpectedValue()).get()) {
					return expr;
				} else {
					return new SQLite3UnaryOperation(UnaryOperator.NOT, expr);
				}
			}
		} while (true);
	}

	//
	private SQLite3Expression generateWindowFunction(List<Column> columns, SQLite3Expression colName,
			boolean allowFilter) {
		StringBuilder sb = new StringBuilder();
		if (Randomly.getBoolean() && allowFilter) {
			appendFilter(columns, sb);
		}
		sb.append(" OVER ");
		sb.append("(");
		if (Randomly.getBoolean()) {
			appendPartitionBy(columns, sb);
		}
		if (Randomly.getBoolean()) {
			sb.append(SQLite3Common.getOrderByAsString(columns, r));
		}
		if (Randomly.getBoolean()) {
			sb.append(" ");
			sb.append(Randomly.fromOptions("RANGE", "ROWS", "GROUPS"));
			sb.append(" ");
			switch (Randomly.fromOptions(FrameSpec.values())) {
			case BETWEEN:
				sb.append("BETWEEN");
				sb.append(" UNBOUNDED PRECEDING AND CURRENT ROW");
				break;
			case UNBOUNDED_PRECEDING:
				sb.append("UNBOUNDED PRECEDING");
				break;
			case CURRENT_ROW:
				sb.append("CURRENT ROW");
				break;
			}
//			sb.append(" BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING");
			if (Randomly.getBoolean()) {
				sb.append(" EXCLUDE ");
				// "CURRENT ROW", "GROUP"
				sb.append(Randomly.fromOptions("NO OTHERS", "TIES"));
			}
		}
		sb.append(")");
		colName = new SQLite3PostfixText(colName, sb.toString(), colName.getExpectedValue());
		errors.add("misuse of aggregate");
		return colName;
	}

	private void appendFilter(List<Column> columns, StringBuilder sb) {
		sb.append(" FILTER (WHERE ");
		sb.append(SQLite3Visitor.asString(generateWhereClauseThatContainsRowValue(columns, rw)));
		sb.append(")");
	}

	private void appendPartitionBy(List<Column> columns, StringBuilder sb) {
		sb.append(" PARTITION BY ");
		for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
			if (i != 0) {
				sb.append(", ");
			}
			String orderingTerm;
			do {
				orderingTerm = SQLite3Common.getOrderingTerm(columns, r);
			} while (orderingTerm.contains("ASC") || orderingTerm.contains("DESC"));
			// TODO investigate
			sb.append(orderingTerm);
		}
	}

	private enum FrameSpec {
		BETWEEN, UNBOUNDED_PRECEDING, CURRENT_ROW
	}

}
