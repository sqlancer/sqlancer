package sqlancer.mariadb;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import sqlancer.QueryAdapter;
import sqlancer.Randomly;
import sqlancer.StateToReproduce.MariaDBStateToReproduce;
import sqlancer.mariadb.MariaDBSchema.MariaDBColumn;
import sqlancer.mariadb.MariaDBSchema.MariaDBDataType;
import sqlancer.mariadb.MariaDBSchema.MariaDBTable;
import sqlancer.mariadb.ast.MariaDBAggregate;
import sqlancer.mariadb.ast.MariaDBAggregate.MariaDBAggregateFunction;
import sqlancer.mariadb.ast.MariaDBColumnName;
import sqlancer.mariadb.ast.MariaDBExpression;
import sqlancer.mariadb.ast.MariaDBPostfixUnaryOperation;
import sqlancer.mariadb.ast.MariaDBPostfixUnaryOperation.MariaDBPostfixUnaryOperator;
import sqlancer.mariadb.ast.MariaDBSelectStatement;
import sqlancer.mariadb.ast.MariaDBSelectStatement.MariaDBSelectType;
import sqlancer.mariadb.ast.MariaDBText;
import sqlancer.mariadb.ast.MariaDBVisitor;
import sqlancer.mariadb.gen.MariaDBExpressionGenerator;

public class MariaDBMetamorphicQuerySynthesizer {

	// SELECT COUNT(*) FROM t0 WHERE <cond>;
	// SELECT SUM(count) FROM (SELECT <cond> IS TRUE as count FROM t0);
	// SELECT (SELECT COUNT(*) FROM t0 WHERE c0 IS NOT 0) = (SELECT COUNT(*) FROM
	// (SELECT c0 is NOT 0 FROM t0));

	private MariaDBSchema s;
	private Randomly r;
	private Connection con;
	private MariaDBStateToReproduce state;
	private String firstQueryString;
	private String secondQueryString;
	private final List<String> errors = new ArrayList<>();

	public MariaDBMetamorphicQuerySynthesizer(MariaDBSchema s, Randomly r, Connection con,
			MariaDBStateToReproduce state) {
		this.s = s;
		this.r = r;
		this.con = con;
		this.state = state;
		errors.add("is out of range");
		// regex
		errors.add("unmatched parentheses");
		errors.add("nothing to repeat at offset");
		errors.add("missing )");
		errors.add("missing terminating ]");
		errors.add("range out of order in character class");
		errors.add("unrecognized character after ");
		errors.add("Got error '(*VERB) not recognized or malformed");
		errors.add("must be followed by");
		errors.add("malformed number or name after");
		errors.add("digit expected after");
	}

	public void generateAndCheck() throws SQLException {
		MariaDBTable randomTable = s.getRandomTable();
		List<MariaDBColumn> columns = randomTable.getColumns();
		MariaDBExpressionGenerator gen = new MariaDBExpressionGenerator(r).setColumns(columns).setCon(con)
				.setState(state);
		MariaDBExpression randomWhereCondition = gen.getRandomExpression();
		List<MariaDBExpression> groupBys = Collections.emptyList(); // getRandomExpressions(columns);
		int firstCount = getFirstQueryCount(con, randomTable, randomWhereCondition, groupBys);
		int secondCount = getSecondQuery(randomTable, randomWhereCondition, groupBys);
		if (firstCount != secondCount && firstCount != NOT_FOUND && secondCount != NOT_FOUND) {
			state.queryString = firstQueryString + ";\n" + secondQueryString + ";";
			throw new AssertionError(firstCount + " " + secondCount);
		}
	}

//	private List<MariaDBExpression> getRandomExpressions(List<Column> columns, Table randomTable) {
//		List<MariaDBExpression> randomExpressions = columns.stream().map(c -> new ColumnName(c, null)).collect(Collectors.toList());
//		if (Randomly.getBoolean()) {
//			for (int i = 0; i < Randomly.smallNumber(); i++) {
//				randomExpressions.add(getRandomWhereCondition(columns, randomTable));
//			}
//		}
//		return randomExpressions;
//	}

	private int getSecondQuery(MariaDBTable randomTable, MariaDBExpression randomWhereCondition,
			List<MariaDBExpression> groupBys) throws SQLException {
		MariaDBSelectStatement select = new MariaDBSelectStatement();
		select.setGroupByClause(groupBys);
		MariaDBPostfixUnaryOperation isTrue = new MariaDBPostfixUnaryOperation(MariaDBPostfixUnaryOperator.IS_TRUE, randomWhereCondition);
		MariaDBText asText = new MariaDBText(isTrue, " as count", false);
		select.setFetchColumns(Arrays.asList(asText));
		select.setFromTables(Arrays.asList(randomTable));
		select.setSelectType(MariaDBSelectType.ALL);
		int secondCount = 0;

		secondQueryString = "SELECT SUM(count) FROM (" + MariaDBVisitor.asString(select) + ") as asdf";
		QueryAdapter q = new QueryAdapter(secondQueryString, errors);
		try (ResultSet rs = q.executeAndGet(con)) {
			if (rs == null) {
				return NOT_FOUND;
			} else {
				while (rs.next()) {
					secondCount = rs.getInt(1);
					rs.getStatement().close();
				}
				rs.getStatement().close();
			}
		}

		return secondCount;
	}

	private int getFirstQueryCount(Connection con, MariaDBTable randomTable, MariaDBExpression randomWhereCondition,
			List<MariaDBExpression> groupBys) throws SQLException {
		MariaDBSelectStatement select = new MariaDBSelectStatement();
		select.setGroupByClause(groupBys);
		// TODO: randomly select column and then = TRUE instead of IS TRUE
		// SELECT COUNT(t1.c3) FROM t1 WHERE (- (t1.c2));
		// SELECT SUM(count) FROM (SELECT ((- (t1.c2)) IS TRUE) as count FROM t1);;
		MariaDBAggregate aggr = new MariaDBAggregate(
				new MariaDBColumnName(new MariaDBColumn("*", MariaDBDataType.INT, false, 0)),
				MariaDBAggregateFunction.COUNT);
		select.setFetchColumns(Arrays.asList(aggr));
		select.setFromTables(Arrays.asList(randomTable));
		select.setWhereClause(randomWhereCondition);
		select.setSelectType(MariaDBSelectType.ALL);
		int firstCount = 0;
		firstQueryString = MariaDBVisitor.asString(select);
		QueryAdapter q = new QueryAdapter(firstQueryString, errors);
		try (ResultSet rs = q.executeAndGet(con)) {
			if (rs == null) {
				firstCount = NOT_FOUND;
			} else {
				rs.next();
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
