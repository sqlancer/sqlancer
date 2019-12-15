package lama.sqlite3.queries;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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
import lama.sqlite3.ast.SQLite3Expression.Join.JoinType;
import lama.sqlite3.ast.SQLite3Expression.SQLite3PostfixText;
import lama.sqlite3.ast.SQLite3Expression.SQLite3PostfixUnaryOperation;
import lama.sqlite3.ast.SQLite3Expression.SQLite3PostfixUnaryOperation.PostfixUnaryOperator;
import lama.sqlite3.ast.SQLite3SelectStatement;
import lama.sqlite3.ast.SQLite3SelectStatement.SelectType;
import lama.sqlite3.gen.SQLite3ExpressionGenerator;
import lama.sqlite3.schema.SQLite3DataType;
import lama.sqlite3.schema.SQLite3Schema;
import lama.sqlite3.schema.SQLite3Schema.Column;
import lama.sqlite3.schema.SQLite3Schema.Table;
import lama.sqlite3.schema.SQLite3Schema.Tables;

public class SQLite3MetamorphicQuerySynthesizer {

	// SELECT COUNT(*) FROM t0 WHERE <cond>;
	// SELECT SUM(count) FROM (SELECT <cond> IS TRUE as count FROM t0);
	// SELECT (SELECT COUNT(*) FROM t0 WHERE c0 IS NOT 0) = (SELECT COUNT(*) FROM
	// (SELECT c0 is NOT 0 FROM t0));

	private SQLite3Schema s;
	private Randomly r;
	private Connection con;
	private SQLite3StateToReproduce state;
	private String firstQueryString;
	private String secondQueryString;
	private final List<String> errors = new ArrayList<>();
	private StateLogger logger;
	private MainOptions options;
	private SQLite3GlobalState globalState;

	public SQLite3MetamorphicQuerySynthesizer(SQLite3Schema s, Randomly r, Connection con,
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
		errors.add("misuse of window function");
		errors.add("second argument to nth_value must be a positive integer");
		errors.add("no such table");
//		errors.add("no such index"); // INDEXED BY 
//		errors.add("no query solution"); // INDEXED BY
	}

	public void generateAndCheck() throws SQLException {
		Tables randomTable = s.getRandomTableNonEmptyTables();
		List<Column> columns = randomTable.getColumns();
		SQLite3Expression randomWhereCondition = getRandomWhereCondition(columns);
		List<SQLite3Expression> groupBys = Collections.emptyList(); // getRandomExpressions(columns);
		List<Table> tables = randomTable.getTables();// Randomly.extractNrRandomColumns(randomTable.getTables(), Math.min(Randomly.smallNumber() + 1, randomTable.getTables().size() - 1));
		List<Join> joinStatements = new ArrayList<>();
		if (Randomly.getBoolean() && tables.size() > 1) {
			int nrJoinClauses = (int) Randomly.getNotCachedInteger(0, tables.size());
			for (int i = 1; i < nrJoinClauses; i++) {
				SQLite3Expression joinClause = getRandomWhereCondition(columns);
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

		}
		int firstCount = getFirstQueryCount(con, tables, randomWhereCondition, groupBys, joinStatements);
		if (firstQueryString.contains("EXISTS")) {
			throw new IgnoreMeException();
		}
		int secondCount = getSecondQuery(tables, randomWhereCondition, groupBys, joinStatements);
//		if (firstCount != NOT_FOUND && secondCount != NOT_FOUND) {
//			if (firstQueryString.contains("MATCH")) {
//				System.out.println(firstQueryString);
//			}
//		}
		if (firstCount != secondCount && firstCount != NOT_FOUND && secondCount != NOT_FOUND) {
			state.queryString = firstQueryString + ";\n" + secondQueryString + ";";
			throw new AssertionError(firstCount + " " + secondCount);
		}
	}

//	private List<SQLite3Expression> getRandomExpressions(List<Column> columns, Table randomTable) {
//		List<SQLite3Expression> randomExpressions = columns.stream().map(c -> new ColumnName(c, null)).collect(Collectors.toList());
//		if (Randomly.getBoolean()) {
//			for (int i = 0; i < Randomly.smallNumber(); i++) {
//				randomExpressions.add(getRandomWhereCondition(columns, randomTable));
//			}
//		}
//		return randomExpressions;
//	}

	private SQLite3Expression getRandomWhereCondition(List<Column> columns) {
		SQLite3ExpressionGenerator gen = new SQLite3ExpressionGenerator(r).setColumns(columns).setGlobalState(globalState)
				.setState(state);
		// FIXME: enable match clause for multiple tables
//		if (randomTable.isVirtual()) {
			if (Randomly.getBoolean()) {
				errors.add("SQL logic error");
				gen.allowMatchClause();
			}
//		}
		return gen.getRandomExpression();
	}

	private int getSecondQuery(List<Table> list, SQLite3Expression randomWhereCondition,
			List<SQLite3Expression> groupBys, List<Join> joinStatements) throws SQLException {
		SQLite3SelectStatement select = new SQLite3SelectStatement();
		select.setGroupByClause(groupBys);
		SQLite3PostfixUnaryOperation isTrue = new SQLite3PostfixUnaryOperation(PostfixUnaryOperator.IS_TRUE, randomWhereCondition);
		SQLite3PostfixText asText = new SQLite3PostfixText(isTrue, " as count", null);
		select.setFetchColumns(Arrays.asList(asText));
		select.setFromTables(list);
		select.setSelectType(SelectType.ALL);
		select.setJoinClauses(joinStatements);
		int secondCount = 0;
		secondQueryString = "SELECT SUM(count) FROM (" + SQLite3Visitor.asString(select) + ")";
		if (options.logEachSelect()) {
			logger.writeCurrent(secondQueryString);
		}
		QueryAdapter q = new QueryAdapter(secondQueryString, errors);
		try (ResultSet rs = q.executeAndGet(con)) {
			if (rs == null) {
				return NOT_FOUND;
			} else {
				while (rs.next()) {
					secondCount = rs.getInt(1);
				}
				rs.getStatement().close();
			}
		}

		return secondCount;
	}

	private int getFirstQueryCount(Connection con, List<Table> list, SQLite3Expression randomWhereCondition,
			List<SQLite3Expression> groupBys, List<Join> joinStatements) throws SQLException {
		SQLite3SelectStatement select = new SQLite3SelectStatement();
		select.setGroupByClause(groupBys);
		// TODO: randomly select column and then = TRUE instead of IS TRUE
		// SELECT COUNT(t1.c3) FROM t1 WHERE (- (t1.c2));
		// SELECT SUM(count) FROM (SELECT ((- (t1.c2)) IS TRUE) as count FROM t1);;
		SQLite3Aggregate aggr = new SQLite3Aggregate(
				new ColumnName(new Column("*", SQLite3DataType.INT, false, false, null), null),
				SQLite3AggregateFunction.COUNT);
		select.setFetchColumns(Arrays.asList(aggr));
		select.setFromTables(list);
		select.setWhereClause(randomWhereCondition);
		select.setSelectType(SelectType.ALL);
		select.setJoinClauses(joinStatements);
		int firstCount = 0;
		firstQueryString = SQLite3Visitor.asString(select);
		if (options.logEachSelect()) {
			logger.writeCurrent(firstQueryString);
		}
		QueryAdapter q = new QueryAdapter(firstQueryString, errors);
		try (ResultSet rs = q.executeAndGet(con)) {
			if (rs == null) {
				firstCount = NOT_FOUND;
			} else {
				firstCount = rs.getInt(1);
				rs.getStatement().close();
			}
		} catch (Exception e) {
			throw new AssertionError(firstQueryString, e);
		}
		return firstCount;
	}

	private final static int NOT_FOUND = -1;

}
