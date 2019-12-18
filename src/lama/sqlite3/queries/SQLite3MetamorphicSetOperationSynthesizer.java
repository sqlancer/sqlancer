package lama.sqlite3.queries;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import lama.IgnoreMeException;
import lama.Main.StateLogger;
import lama.MainOptions;
import lama.QueryAdapter;
import lama.Randomly;
import lama.StateToReproduce.SQLite3StateToReproduce;
import lama.sqlite3.SQLite3Errors;
import lama.sqlite3.SQLite3Provider.SQLite3GlobalState;
import lama.sqlite3.SQLite3Visitor;
import lama.sqlite3.ast.SQLite3Aggregate;
import lama.sqlite3.ast.SQLite3Aggregate.SQLite3AggregateFunction;
import lama.sqlite3.ast.SQLite3Expression;
import lama.sqlite3.ast.SQLite3Expression.ColumnName;
import lama.sqlite3.ast.SQLite3Expression.Join;
import lama.sqlite3.ast.SQLite3Expression.SQLite3PostfixUnaryOperation;
import lama.sqlite3.ast.SQLite3Expression.SQLite3PostfixUnaryOperation.PostfixUnaryOperator;
import lama.sqlite3.ast.SQLite3SelectStatement;
import lama.sqlite3.ast.SQLite3SelectStatement.SelectType;
import lama.sqlite3.ast.SQLite3UnaryOperation;
import lama.sqlite3.ast.SQLite3UnaryOperation.UnaryOperator;
import lama.sqlite3.gen.SQLite3ExpressionGenerator;
import lama.sqlite3.schema.SQLite3DataType;
import lama.sqlite3.schema.SQLite3Schema;
import lama.sqlite3.schema.SQLite3Schema.Column;
import lama.sqlite3.schema.SQLite3Schema.Table;
import lama.sqlite3.schema.SQLite3Schema.Tables;

public class SQLite3MetamorphicSetOperationSynthesizer {

	// SELECT COUNT(*) FROM t0 WHERE <cond>;
	// SELECT SUM(count) FROM (SELECT <cond> IS TRUE as count FROM t0);
	// SELECT (SELECT COUNT(*) FROM t0 WHERE c0 IS NOT 0) = (SELECT COUNT(*) FROM
	// (SELECT c0 is NOT 0 FROM t0));

	private SQLite3Schema s;
	private Randomly r;
	private Connection con;
	private SQLite3StateToReproduce state;
	private final List<String> queries = new ArrayList<>();
	private final List<String> errors = new ArrayList<>();
	private StateLogger logger;
	private MainOptions options;
	private SQLite3GlobalState globalState;

	public SQLite3MetamorphicSetOperationSynthesizer(SQLite3Schema s, Randomly r, Connection con,
			SQLite3StateToReproduce state, StateLogger logger, MainOptions options, SQLite3GlobalState globalState) {
		this.s = s;
		this.r = r;
		this.con = con;
		this.state = state;
		this.logger = logger;
		this.options = options;
		this.globalState = globalState;
		SQLite3Errors.addExpectedExpressionErrors(errors);
		SQLite3Errors.addMatchQueryErrors(errors);

		// aggregate
		errors.add("misuse of aggregate");
		errors.add("second argument to nth_value must be a positive integer");
		errors.add("no such table");
		errors.add("generated column loop on");
//		errors.add("no such index"); // INDEXED BY 
//		errors.add("no query solution"); // INDEXED BY
	}

	public void generateAndCheck() throws SQLException {
		queries.clear();
		Tables randomTable = s.getRandomTableNonEmptyTables();
		List<Column> columns = randomTable.getColumns();
		SQLite3Expression whereConditionA = getRandomWhereCondition(columns);
		SQLite3Expression whereConditionB = getRandomWhereCondition(columns);

//		List<SQLite3Expression> groupBys = Collections.emptyList(); //getRandomExpressions(columns);
		List<Table> tables = randomTable.getTables();
//		List<Join> joinStatements = new ArrayList<>();
//		if (Randomly.getBoolean()) {
//			int nrJoinClauses =  (int) Randomly.getNotCachedInteger(0, tables.size());
//			for (int i = 1; i < nrJoinClauses; i++) {
//				SQLite3Expression joinClause = getRandomWhereCondition(columns);
//				Table table = Randomly.fromList(tables);
//				tables.remove(table);
//				JoinType options;
//				options = Randomly.fromOptions(JoinType.INNER, JoinType.CROSS, JoinType.OUTER);
//				if (options == JoinType.OUTER && tables.size() > 2) {
//					errors.add("ON clause references tables to its right");
//				}
//				Join j = new SQLite3Expression.Join(table, joinClause, options);
//				joinStatements.add(j);
//			}
//			
//		}

		String conditionAsQueryA = conditionAsQuery(randomTable, whereConditionA);
		String conditionAsQueryB = conditionAsQuery(randomTable, whereConditionB);
		String left1 = "SELECT COUNT(*) FROM (" + conditionAsQueryA + " EXCEPT " + conditionAsQueryB + ")";
		String left2 = "SELECT COUNT(*) FROM (" + conditionAsQueryA + " INTERSECT " + conditionAsQueryB + ")";
		String right = "SELECT COUNT(*) FROM (" + conditionAsQueryA + ")";
		try {
			int leftCount = getCount(left1);
			int leftCount2 = getCount(left2);
			int rightCount = getCount(right);

			if (leftCount == NOT_FOUND || leftCount2 == NOT_FOUND || rightCount == NOT_FOUND) {
				throw new IgnoreMeException();
			}
			if (leftCount + leftCount2 != rightCount) {
				throw new AssertionError(leftCount + " " + leftCount2 + " " + rightCount + "\n" + getQueriesAsString());
			}
//		String right = "SELECT COUNT(*) FROM(" + conditionAsQuery(randomTable, new Sqlite3BinaryOperation(whereConditionA, new SQLite3UnaryOperation(UnaryOperator.NOT, whereConditionB), BinaryOperator.AND)) + ")";
//		int leftCount = getCount(left1);
//		int rightCount = getCount(right);
//		if (leftCount == NOT_FOUND || rightCount == NOT_FOUND) {
//			throw new IgnoreMeException();
//		}
//		if (leftCount != rightCount) {
//			throw new IgnoreMeException();
//		}
		} catch (Exception e) {
			if (e instanceof IgnoreMeException) {
				throw e;
			}
			throw new AssertionError(getQueriesAsString(), e);
		}
	}

	private String getQueriesAsString() {
		return queries.stream().collect(Collectors.joining("\n"));
	}

	private int getCount(String s) throws SQLException {
		queries.add(s);
		if (options.logEachSelect()) {
			logger.writeCurrent(s);
		}
		QueryAdapter q = new QueryAdapter(s, errors);
		try (ResultSet rs = q.executeAndGet(con)) {
			if (rs == null) {
				return NOT_FOUND;
			} else {
				if (rs.next()) {
					return rs.getInt(1);
				}
				rs.getStatement().close();
			}
		}
		return NOT_FOUND;
	}

	private String conditionAsQuery(Tables randomTable, SQLite3Expression randomWhereCondition) {
		SQLite3SelectStatement select = new SQLite3SelectStatement();
		ColumnName count =
				// new SQLite3Aggregate(
				new ColumnName(new Column("*", SQLite3DataType.INT, false, false, null), null); // , null),
//				((SQLite3AggregateFunction.COUNT);
		select.setFetchColumns(Arrays.asList(count));
		select.setFromTables(randomTable.getTables());
		select.setSelectType(SelectType.DISTINCT);
		select.setWhereClause(randomWhereCondition);
		String totalString = SQLite3Visitor.asString(select);
		return totalString;
	}

	private int getTotalCount(List<Table> list, List<SQLite3Expression> groupBys, List<Join> joinStatements)
			throws SQLException {
		SQLite3SelectStatement select = new SQLite3SelectStatement();
		select.setGroupByClause(groupBys);
		SQLite3Aggregate count = new SQLite3Aggregate(
				new ColumnName(new Column("*", SQLite3DataType.INT, false, false, null), null),
				SQLite3AggregateFunction.COUNT);
		select.setFetchColumns(Arrays.asList(count));
		select.setFromTables(list);
		select.setSelectType(SelectType.ALL);
		select.setJoinClauses(joinStatements);
		int totalCount = 0;

		String totalString = SQLite3Visitor.asString(select);
		queries.add(totalString);
		QueryAdapter q = new QueryAdapter(totalString, errors);
		try (ResultSet rs = q.executeAndGet(con)) {
			if (rs == null) {
				return NOT_FOUND;
			} else {
				if (rs.next()) {
					totalCount = rs.getInt(1);
				}
				rs.getStatement().close();
			}
		}
		return totalCount;
	}

	private enum Mode {
		TRUE, FALSE, ISNULL;
	}

	private int getSubsetCount(List<Table> list, SQLite3Expression condition, List<SQLite3Expression> groupBys,
			List<Join> joinStatements, Mode m) throws SQLException {
		SQLite3SelectStatement select = new SQLite3SelectStatement();
		select.setGroupByClause(groupBys);
		SQLite3Aggregate aggr = new SQLite3Aggregate(
				new ColumnName(new Column("*", SQLite3DataType.INT, false, false, null), null),
				SQLite3AggregateFunction.COUNT);
		select.setFetchColumns(Arrays.asList(aggr));
		select.setFromTables(list);
		select.setSelectType(SelectType.ALL);
		select.setJoinClauses(joinStatements);
		if (m == Mode.TRUE) {
			select.setWhereClause(condition);
		} else if (m == Mode.FALSE) {
			select.setWhereClause(new SQLite3UnaryOperation(UnaryOperator.NOT, condition));
		} else if (m == Mode.ISNULL) {
			select.setWhereClause(new SQLite3PostfixUnaryOperation(PostfixUnaryOperator.ISNULL, condition));
		}
		int totalCount = 0;

		String totalString = SQLite3Visitor.asString(select);
		queries.add(totalString);
		QueryAdapter q = new QueryAdapter(totalString, errors);
		try (ResultSet rs = q.executeAndGet(con)) {
			if (rs == null) {
				return NOT_FOUND;
			} else {
				if (rs.next()) {
					totalCount = rs.getInt(1);
				}
				rs.getStatement().close();
			}
		}
		return totalCount;
	}

	private SQLite3Expression getRandomWhereCondition(List<Column> columns) {
		SQLite3ExpressionGenerator gen = new SQLite3ExpressionGenerator(globalState).setColumns(columns);
		// FIXME: enable match clause for multiple tables
//		if (randomTable.isVirtual()) {
//			gen.allowMatchClause();
//		}
		return gen.getRandomExpression();
	}

	private final static int NOT_FOUND = -1;

}
