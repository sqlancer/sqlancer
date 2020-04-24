package sqlancer.sqlite3.queries;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import sqlancer.IgnoreMeException;
import sqlancer.Main.StateLogger;
import sqlancer.MainOptions;
import sqlancer.QueryAdapter;
import sqlancer.Randomly;
import sqlancer.StateToReproduce.SQLite3StateToReproduce;
import sqlancer.TestOracle;
import sqlancer.sqlite3.SQLite3Errors;
import sqlancer.sqlite3.SQLite3Provider.SQLite3GlobalState;
import sqlancer.sqlite3.SQLite3Visitor;
import sqlancer.sqlite3.ast.SQLite3Expression;
import sqlancer.sqlite3.ast.SQLite3Expression.Join;
import sqlancer.sqlite3.ast.SQLite3Expression.SQLite3ColumnName;
import sqlancer.sqlite3.ast.SQLite3Expression.SQLite3PostfixText;
import sqlancer.sqlite3.ast.SQLite3Expression.SQLite3PostfixUnaryOperation;
import sqlancer.sqlite3.ast.SQLite3Expression.SQLite3PostfixUnaryOperation.PostfixUnaryOperator;
import sqlancer.sqlite3.ast.SQLite3Select;
import sqlancer.sqlite3.ast.SQLite3Select.SelectType;
import sqlancer.sqlite3.gen.SQLite3Common;
import sqlancer.sqlite3.gen.SQLite3ExpressionGenerator;
import sqlancer.sqlite3.schema.SQLite3Schema;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Column;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Table;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Tables;

public class SQLite3MetamorphicQuerySynthesizer implements TestOracle {

	// SELECT COUNT(*) FROM t0 WHERE <cond>;
	// SELECT SUM(count) FROM (SELECT <cond> IS TRUE as count FROM t0);
	// SELECT (SELECT COUNT(*) FROM t0 WHERE c0 IS NOT 0) = (SELECT COUNT(*) FROM
	// (SELECT c0 is NOT 0 FROM t0));
	private final static int NOT_FOUND = -1;
	private SQLite3ExpressionGenerator gen;
	private SQLite3Schema s;
	private Connection con;
	private SQLite3StateToReproduce state;
	private String firstQueryString;
	private String secondQueryString;
	private final Set<String> errors = new HashSet<>();
	private StateLogger logger;
	private MainOptions options;
	private SQLite3GlobalState globalState;
	private SQLite3Column randomColumnToCheck;
	Set<String> firstValues = new HashSet<>();
	Set<String> secondValues = new HashSet<>();

	public SQLite3MetamorphicQuerySynthesizer(SQLite3GlobalState globalState) {
		this.s = globalState.getSchema();
		this.con = globalState.getConnection();
		this.state = (SQLite3StateToReproduce) globalState.getState();
		this.logger = globalState.getLogger();
		this.options = globalState.getOptions();
		this.globalState = globalState;
		SQLite3Errors.addExpectedExpressionErrors(errors);
		SQLite3Errors.addMatchQueryErrors(errors);
		SQLite3Errors.addQueryErrors(errors);
		// aggregate
		errors.add("misuse of aggregate");
		errors.add("misuse of window function");
		errors.add("second argument to nth_value must be a positive integer");
		errors.add("no such table");
		// FIXME implement
		errors.add("no query solution"); // INDEXED BY
		errors.add("unable to use function MATCH in the requested context");
	}

	@Override
	public void check() throws SQLException {
		firstValues.clear();
		secondValues.clear();
		SQLite3Tables randomTables = s.getRandomTableNonEmptyTables();
		List<SQLite3Column> columns = randomTables.getColumns();
		gen = new SQLite3ExpressionGenerator(globalState).setColumns(columns);
		SQLite3Expression randomWhereCondition = getRandomWhereCondition(columns);
		List<SQLite3Expression> groupBys = gen.getRandomExpressions(Randomly.smallNumber());
		List<SQLite3Table> tables = randomTables.getTables();
		List<Join> joinStatements = gen.getRandomJoinClauses(tables);
		List<SQLite3Expression> tableRefs = SQLite3Common.getTableRefs(tables, s);
		randomColumnToCheck = Randomly.fromList(randomTables.getColumns());
		int firstCount = getFirstQueryCount(con, tableRefs, randomWhereCondition, groupBys, joinStatements);
		if (firstQueryString.contains("EXISTS")) {
			throw new IgnoreMeException();
		}
		int secondCount = getSecondQuery(tableRefs, randomWhereCondition, groupBys, joinStatements);
		if (firstCount != secondCount && firstCount != NOT_FOUND && secondCount != NOT_FOUND) {
			state.queryString = firstQueryString + ";\n" + secondQueryString + ";";
			throw new AssertionError(firstCount + " " + secondCount);
		}
		if (firstCount != NOT_FOUND && secondCount != NOT_FOUND && (!firstValues.containsAll(secondValues) || !secondValues.containsAll(firstValues))) {
			state.queryString = firstQueryString + ";\n" + secondQueryString + ";";
			throw new AssertionError(firstCount + " " + secondCount + "\n" + firstValues + "\n" + secondValues);
		}
	}
		
	private SQLite3Expression getRandomWhereCondition(List<SQLite3Column> columns) {
		if (Randomly.getBoolean()) {
			errors.add("SQL logic error");
			gen.allowMatchClause();
		}
		return gen.generateExpression();
	}

	private int getSecondQuery(List<SQLite3Expression> fromList, SQLite3Expression randomWhereCondition,
			List<SQLite3Expression> groupBys, List<Join> joinStatements) throws SQLException {
		SQLite3Select select = new SQLite3Select();
		setRandomOrderBy(select);
		SQLite3PostfixUnaryOperation isTrue = new SQLite3PostfixUnaryOperation(PostfixUnaryOperator.IS_TRUE,
				randomWhereCondition);
		SQLite3PostfixText asText = new SQLite3PostfixText(isTrue, " as count", null);
		select.setFetchColumns(Arrays.asList(asText, new SQLite3ColumnName(randomColumnToCheck, null)));
		select.setFromTables(fromList);
		select.setSelectType(SelectType.ALL);
		select.setJoinClauses(joinStatements);
		int secondCount = 0;
		secondQueryString = SQLite3Visitor.asString(select);
		if (options.logEachSelect()) {
			logger.writeCurrent(secondQueryString);
		}
		QueryAdapter q = new QueryAdapter(secondQueryString, errors);
		try (ResultSet rs = q.executeAndGet(con)) {
			if (rs == null) {
				return NOT_FOUND;
			} else {
				try {
					while (rs.next()) {
						int val = rs.getInt(1);
						if (val == 1) {
							secondCount++;
							String string = rs.getString(2);
							if (string == null) {
								secondValues.add("null");
							} else {
								secondValues.add(string);
							}
						}
					}
				} catch (SQLException e) {
					throw new IgnoreMeException();
				}
				rs.getStatement().close();
			}
		} catch (Exception e) {
			if (e instanceof IgnoreMeException) {
				throw e;
			}
			throw new AssertionError(secondQueryString, e);
		}
		return secondCount;
	}

	private int getFirstQueryCount(Connection con, List<SQLite3Expression> fromList,
			SQLite3Expression randomWhereCondition, List<SQLite3Expression> groupBys, List<Join> joinStatements)
			throws SQLException {
		SQLite3Select select = new SQLite3Select();
		// TODO: readd group by (removed due to INTERSECT/UNION)
//		select.setGroupByClause(groupBys);
		setRandomOrderBy(select);
		// TODO: randomly select column and then = TRUE instead of IS TRUE
		// SELECT COUNT(t1.c3) FROM t1 WHERE (- (t1.c2));
		// SELECT SUM(count) FROM (SELECT ((- (t1.c2)) IS TRUE) as count FROM t1);;
		SQLite3ColumnName aggr = new SQLite3ColumnName(randomColumnToCheck, null);
		select.setFetchColumns(Arrays.asList(aggr));
		select.setFromTables(fromList);
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
				try {
					while (rs.next()) {
						firstCount += 1;
						firstValues.add(String.valueOf(rs.getString(1)));
					}
				} catch (Exception e) {
					q.checkException(e);
					firstCount = NOT_FOUND;
				}
				rs.getStatement().close();
			}
		} catch (Exception e) {
			throw new AssertionError(firstQueryString, e);
		}
		return firstCount;
	}

	private void setRandomOrderBy(SQLite3Select select) {
		if (Randomly.getBoolean()) {
			select.setOrderByExpressions(gen.generateOrderBys());
		}
	}

}
