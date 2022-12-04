package sqlancer.yugabyte.ysql.oracle;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.oracle.NoRECBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.yugabyte.ysql.YSQLCompoundDataType;
import sqlancer.yugabyte.ysql.YSQLErrors;
import sqlancer.yugabyte.ysql.YSQLGlobalState;
import sqlancer.yugabyte.ysql.YSQLSchema;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLColumn;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLDataType;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLTable;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLTables;
import sqlancer.yugabyte.ysql.YSQLVisitor;
import sqlancer.yugabyte.ysql.ast.YSQLCastOperation;
import sqlancer.yugabyte.ysql.ast.YSQLColumnValue;
import sqlancer.yugabyte.ysql.ast.YSQLExpression;
import sqlancer.yugabyte.ysql.ast.YSQLJoin;
import sqlancer.yugabyte.ysql.ast.YSQLPostfixText;
import sqlancer.yugabyte.ysql.ast.YSQLSelect;
import sqlancer.yugabyte.ysql.gen.YSQLExpressionGenerator;
import sqlancer.yugabyte.ysql.oracle.tlp.YSQLTLPBase;

public class YSQLNoRECOracle extends NoRECBase<YSQLGlobalState> implements TestOracle<YSQLGlobalState> {

    private final YSQLSchema s;

    public YSQLNoRECOracle(YSQLGlobalState globalState) {
        super(globalState);
        this.s = globalState.getSchema();
        YSQLErrors.addCommonExpressionErrors(errors);
        YSQLErrors.addCommonFetchErrors(errors);
    }

    public static List<YSQLJoin> getJoinStatements(YSQLGlobalState globalState, List<YSQLColumn> columns,
            List<YSQLTable> tables) {
        List<YSQLJoin> joinStatements = new ArrayList<>();
        YSQLExpressionGenerator gen = new YSQLExpressionGenerator(globalState).setColumns(columns);
        for (int i = 1; i < tables.size(); i++) {
            YSQLExpression joinClause = gen.generateExpression(YSQLDataType.BOOLEAN);
            YSQLTable table = Randomly.fromList(tables);
            tables.remove(table);
            YSQLJoin.YSQLJoinType options = YSQLJoin.YSQLJoinType.getRandom();
            YSQLJoin j = new YSQLJoin(new YSQLSelect.YSQLFromTable(table, Randomly.getBoolean()), joinClause, options);
            joinStatements.add(j);
        }
        // JOIN subqueries
        for (int i = 0; i < Randomly.smallNumber(); i++) {
            YSQLTables subqueryTables = globalState.getSchema().getRandomTableNonEmptyTables();
            YSQLSelect.YSQLSubquery subquery = YSQLTLPBase.createSubquery(globalState, String.format("sub%d", i),
                    subqueryTables);
            YSQLExpression joinClause = gen.generateExpression(YSQLDataType.BOOLEAN);
            YSQLJoin.YSQLJoinType options = YSQLJoin.YSQLJoinType.getRandom();
            YSQLJoin j = new YSQLJoin(subquery, joinClause, options);
            joinStatements.add(j);
        }
        return joinStatements;
    }

    @Override
    public void check() throws SQLException {
        YSQLTables randomTables = s.getRandomTableNonEmptyTables();
        List<YSQLColumn> columns = randomTables.getColumns();
        YSQLExpression randomWhereCondition = getRandomWhereCondition(columns);
        List<YSQLTable> tables = randomTables.getTables();

        List<YSQLJoin> joinStatements = getJoinStatements(state, columns, tables);
        List<YSQLExpression> fromTables = tables.stream()
                .map(t -> new YSQLSelect.YSQLFromTable(t, Randomly.getBoolean())).collect(Collectors.toList());
        int secondCount = getUnoptimizedQueryCount(fromTables, randomWhereCondition, joinStatements);
        int firstCount = getOptimizedQueryCount(fromTables, columns, randomWhereCondition, joinStatements);
        if (firstCount == -1 || secondCount == -1) {
            throw new IgnoreMeException();
        }
        if (firstCount != secondCount) {
            String queryFormatString = "-- %s;\n-- count: %d";
            String firstQueryStringWithCount = String.format(queryFormatString, optimizedQueryString, firstCount);
            String secondQueryStringWithCount = String.format(queryFormatString, unoptimizedQueryString, secondCount);
            state.getState().getLocalState()
                    .log(String.format("%s\n%s", firstQueryStringWithCount, secondQueryStringWithCount));
            String assertionMessage = String.format("the counts mismatch (%d and %d)!\n%s\n%s", firstCount, secondCount,
                    firstQueryStringWithCount, secondQueryStringWithCount);
            throw new AssertionError(assertionMessage);
        }
    }

    private YSQLExpression getRandomWhereCondition(List<YSQLColumn> columns) {
        return new YSQLExpressionGenerator(state).setColumns(columns).generateExpression(YSQLDataType.BOOLEAN);
    }

    private int getUnoptimizedQueryCount(List<YSQLExpression> fromTables, YSQLExpression randomWhereCondition,
            List<YSQLJoin> joinStatements) throws SQLException {
        YSQLSelect select = new YSQLSelect();
        YSQLCastOperation isTrue = new YSQLCastOperation(randomWhereCondition,
                YSQLCompoundDataType.create(YSQLDataType.INT));
        YSQLPostfixText asText = new YSQLPostfixText(isTrue, " as count", null, YSQLDataType.INT);
        select.setFetchColumns(Collections.singletonList(asText));
        select.setFromList(fromTables);
        select.setSelectType(YSQLSelect.SelectType.ALL);
        select.setJoinClauses(joinStatements);
        int secondCount = 0;
        unoptimizedQueryString = "SELECT SUM(count) FROM (" + YSQLVisitor.asString(select) + ") as res";
        if (options.logEachSelect()) {
            logger.writeCurrent(unoptimizedQueryString);
        }
        errors.add("canceling statement due to statement timeout");
        SQLQueryAdapter q = new SQLQueryAdapter(unoptimizedQueryString, errors);
        SQLancerResultSet rs;
        try {
            rs = q.executeAndGet(state);
        } catch (Exception e) {
            throw new AssertionError(unoptimizedQueryString, e);
        }
        if (rs == null) {
            return -1;
        }
        if (rs.next()) {
            secondCount += rs.getLong(1);
        }
        rs.close();
        return secondCount;
    }

    private int getOptimizedQueryCount(List<YSQLExpression> randomTables, List<YSQLColumn> columns,
            YSQLExpression randomWhereCondition, List<YSQLJoin> joinStatements) throws SQLException {
        YSQLSelect select = new YSQLSelect();
        YSQLColumnValue allColumns = new YSQLColumnValue(Randomly.fromList(columns), null);
        select.setFetchColumns(Arrays.asList(allColumns));
        select.setFromList(randomTables);
        select.setWhereClause(randomWhereCondition);
        if (Randomly.getBooleanWithSmallProbability()) {
            select.setOrderByExpressions(new YSQLExpressionGenerator(state).setColumns(columns).generateOrderBy());
        }
        select.setSelectType(YSQLSelect.SelectType.ALL);
        select.setJoinClauses(joinStatements);
        int firstCount = 0;
        try (Statement stat = con.createStatement()) {
            optimizedQueryString = YSQLVisitor.asString(select);
            if (options.logEachSelect()) {
                logger.writeCurrent(optimizedQueryString);
            }
            try (ResultSet rs = stat.executeQuery(optimizedQueryString)) {
                while (rs.next()) {
                    firstCount++;
                }
            }
        } catch (SQLException e) {
            throw new IgnoreMeException();
        }
        return firstCount;
    }

}
