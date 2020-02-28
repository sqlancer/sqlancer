package lama.sqlite3.queries;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
import lama.sqlite3.ast.SQLite3Expression.Join;
import lama.sqlite3.ast.SQLite3Expression.Join.JoinType;
import lama.sqlite3.ast.SQLite3Expression.SQLite3ColumnName;
import lama.sqlite3.ast.SQLite3Expression.SQLite3PostfixText;
import lama.sqlite3.ast.SQLite3Expression.SQLite3PostfixUnaryOperation;
import lama.sqlite3.ast.SQLite3Expression.SQLite3PostfixUnaryOperation.PostfixUnaryOperator;
import lama.sqlite3.ast.SQLite3Expression.Sqlite3BinaryOperation;
import lama.sqlite3.ast.SQLite3SelectStatement;
import lama.sqlite3.ast.SQLite3SelectStatement.SelectType;
import lama.sqlite3.gen.SQLite3Common;
import lama.sqlite3.gen.SQLite3ExpressionGenerator;
import lama.sqlite3.schema.SQLite3Schema;
import lama.sqlite3.schema.SQLite3Schema.SQLite3Column;
import lama.sqlite3.schema.SQLite3Schema.Table;
import lama.sqlite3.schema.SQLite3Schema.Tables;

public class SQLite3MetamorphicQuerySynthesizer implements SQLite3TestGenerator {

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

	public SQLite3MetamorphicQuerySynthesizer(SQLite3GlobalState globalState) {
		this.s = globalState.getSchema();
		this.con = globalState.getConnection();
		this.state = globalState.getState();
		this.logger = globalState.getLogger();
		this.options = globalState.getMainOptions();
		this.globalState = globalState;
		SQLite3Errors.addExpectedExpressionErrors(errors);
		SQLite3Errors.addMatchQueryErrors(errors);
		// aggregate
		errors.add("misuse of aggregate");
		errors.add("misuse of window function");
		errors.add("second argument to nth_value must be a positive integer");
		errors.add("no such table");
		errors.add("ON clause references tables to its right");
		// FIXME implement
		errors.add("no query solution"); // INDEXED BY
		errors.add("unable to use function MATCH in the requested context");
	}

	@Override
	public void check() throws SQLException {
		Tables randomTables = s.getRandomTableNonEmptyTables();
		List<SQLite3Column> columns = randomTables.getColumns();
		gen = new SQLite3ExpressionGenerator(globalState).setColumns(columns);
		SQLite3Expression randomWhereCondition = getRandomWhereCondition(columns);
		List<SQLite3Expression> groupBys = gen.getRandomExpressions(Randomly.smallNumber());
		List<Table> tables = randomTables.getTables();
		List<Join> joinStatements = gen.getRandomJoinClauses(tables);
		List<SQLite3Expression> tableRefs = SQLite3Common.getTableRefs(tables, s);
		int firstCount = getFirstQueryCount(con, tableRefs, randomWhereCondition, groupBys, joinStatements);
		if (firstQueryString.contains("EXISTS")) {
			throw new IgnoreMeException();
		}
		if (Randomly.getBoolean()) {
			randomWhereCondition = mergeJoinExpressions(randomWhereCondition, joinStatements);
			for (Join j : joinStatements) {
//				if (j.getType() != JoinType.OUTER) {
					tables.add(j.getTable());
//				}
				if (j.getTable().getNrRows() == 0 && j.getType() == JoinType.INNER) {
					throw new IgnoreMeException();
				}
			}
			tableRefs = SQLite3Common.getTableRefs(tables, s);
			joinStatements.clear();
		}
		int secondCount = getSecondQuery(tableRefs, randomWhereCondition, groupBys, joinStatements);
		if (firstCount != secondCount && firstCount != NOT_FOUND && secondCount != NOT_FOUND) {
			state.queryString = firstQueryString + ";\n" + secondQueryString + ";";
			throw new AssertionError(firstCount + " " + secondCount);
		}
	}

	private SQLite3Expression mergeJoinExpressions(SQLite3Expression randomWhereCondition, List<Join> joinStatements) {
		for (Join j : joinStatements) {
			switch (j.getType()) {
			case CROSS:
			case INNER:
				randomWhereCondition = new Sqlite3BinaryOperation(j.getOnClause(), randomWhereCondition, Sqlite3BinaryOperation.BinaryOperator.AND);
				break;
			case OUTER:
				throw new IgnoreMeException();
//				randomWhereCondition = new Sqlite3BinaryOperation(j.getOnClause(), randomWhereCondition, Sqlite3BinaryOperation.BinaryOperator.OR);
//				break;
			default:
				throw new IgnoreMeException();
			}
		}
		return randomWhereCondition;
	}

	private SQLite3Expression getRandomWhereCondition(List<SQLite3Column> columns) {
		if (Randomly.getBoolean()) {
			errors.add("SQL logic error");
			gen.allowMatchClause();
		}
		return gen.getRandomExpression();
	}

	private int getSecondQuery(List<SQLite3Expression> fromList, SQLite3Expression randomWhereCondition,
			List<SQLite3Expression> groupBys, List<Join> joinStatements) throws SQLException {
		SQLite3SelectStatement select = new SQLite3SelectStatement();
		setRandomOrderBy(select);
		SQLite3PostfixUnaryOperation isTrue = new SQLite3PostfixUnaryOperation(PostfixUnaryOperator.IS_TRUE,
				randomWhereCondition);
		SQLite3PostfixText asText = new SQLite3PostfixText(isTrue, " as count", null);
		select.setFetchColumns(Arrays.asList(asText));
		select.setFromTables(fromList);
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
				if (rs.next()) {
					secondCount = rs.getInt(1);
				}
				rs.getStatement().close();
			}
		} catch (Exception e) {
			throw new AssertionError(secondQueryString, e);
		}
		return secondCount;
	}

	private int getFirstQueryCount(Connection con, List<SQLite3Expression> fromList,
			SQLite3Expression randomWhereCondition, List<SQLite3Expression> groupBys, List<Join> joinStatements)
			throws SQLException {
		SQLite3SelectStatement select = new SQLite3SelectStatement();
		// TODO: readd group by (removed due to INTERSECT/UNION)
//		select.setGroupByClause(groupBys);
		setRandomOrderBy(select);
		// TODO: randomly select column and then = TRUE instead of IS TRUE
		// SELECT COUNT(t1.c3) FROM t1 WHERE (- (t1.c2));
		// SELECT SUM(count) FROM (SELECT ((- (t1.c2)) IS TRUE) as count FROM t1);;
		SQLite3Aggregate aggr = new SQLite3Aggregate(Arrays.asList(SQLite3ColumnName.createDummy("*")),
				SQLite3AggregateFunction.COUNT);
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
						firstCount += rs.getInt(1);
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

	private void setRandomOrderBy(SQLite3SelectStatement select) {
		if (Randomly.getBoolean()) {
			select.setOrderByClause(gen.generateOrderingTerms());
		}
	}

}
