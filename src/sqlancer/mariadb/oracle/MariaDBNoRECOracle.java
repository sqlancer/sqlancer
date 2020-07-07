package sqlancer.mariadb.oracle;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import sqlancer.IgnoreMeException;
import sqlancer.QueryAdapter;
import sqlancer.Randomly;
import sqlancer.StateToReproduce;
import sqlancer.mariadb.MariaDBProvider.MariaDBGlobalState;
import sqlancer.mariadb.MariaDBSchema;
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

public class MariaDBNoRECOracle {

    private final MariaDBSchema s;
    private final Randomly r;
    private final Connection con;
    private String firstQueryString;
    private String secondQueryString;
    private final List<String> errors = new ArrayList<>();
    private static final int NOT_FOUND = -1;
    private final StateToReproduce state;
    private final MariaDBGlobalState globalState;

    public MariaDBNoRECOracle(MariaDBGlobalState globalState) {
        this.s = globalState.getSchema();
        this.r = globalState.getRandomly();
        this.con = globalState.getConnection();
        this.state = globalState.getState();
        this.globalState = globalState;
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
        int optimizedCount = getOptimizedQuery(randomTable, randomWhereCondition, groupBys);
        int unoptimizedCount = getUnoptimizedQuery(randomTable, randomWhereCondition, groupBys);
        if (optimizedCount == NOT_FOUND || unoptimizedCount == NOT_FOUND) {
            throw new IgnoreMeException();
        }
        if (optimizedCount != unoptimizedCount) {
            state.queryString = firstQueryString + ";\n" + secondQueryString + ";";
            throw new AssertionError(optimizedCount + " " + unoptimizedCount);
        }
    }

    private int getUnoptimizedQuery(MariaDBTable randomTable, MariaDBExpression randomWhereCondition,
            List<MariaDBExpression> groupBys) throws SQLException {
        MariaDBSelectStatement select = new MariaDBSelectStatement();
        select.setGroupByClause(groupBys);
        MariaDBPostfixUnaryOperation isTrue = new MariaDBPostfixUnaryOperation(MariaDBPostfixUnaryOperator.IS_TRUE,
                randomWhereCondition);
        MariaDBText asText = new MariaDBText(isTrue, " as count", false);
        select.setFetchColumns(Arrays.asList(asText));
        select.setFromTables(Arrays.asList(randomTable));
        select.setSelectType(MariaDBSelectType.ALL);
        int secondCount = 0;

        secondQueryString = "SELECT SUM(count) FROM (" + MariaDBVisitor.asString(select) + ") as asdf";
        QueryAdapter q = new QueryAdapter(secondQueryString, errors);
        try (ResultSet rs = q.executeAndGet(globalState)) {
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

    private int getOptimizedQuery(MariaDBTable randomTable, MariaDBExpression randomWhereCondition,
            List<MariaDBExpression> groupBys) throws SQLException {
        MariaDBSelectStatement select = new MariaDBSelectStatement();
        select.setGroupByClause(groupBys);
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
        try (ResultSet rs = q.executeAndGet(globalState)) {
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

}
