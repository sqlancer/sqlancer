package sqlancer.clickhouse.oracle.norec;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.clickhouse.ClickHouseErrors;
import sqlancer.clickhouse.ClickHouseProvider.ClickHouseGlobalState;
import sqlancer.clickhouse.ClickHouseSchema;
import sqlancer.clickhouse.ClickHouseSchema.ClickHouseColumn;
import sqlancer.clickhouse.ClickHouseSchema.ClickHouseLancerDataType;
import sqlancer.clickhouse.ClickHouseSchema.ClickHouseTable;
import sqlancer.clickhouse.ClickHouseSchema.ClickHouseTables;
import sqlancer.clickhouse.ClickHouseToStringVisitor;
import sqlancer.clickhouse.ast.ClickHouseAliasOperation;
import sqlancer.clickhouse.ast.ClickHouseColumnReference;
import sqlancer.clickhouse.ast.ClickHouseExpression;
import sqlancer.clickhouse.ast.ClickHouseSelect;
import sqlancer.clickhouse.ast.ClickHouseTableReference;
import sqlancer.clickhouse.gen.ClickHouseExpressionGenerator;
import sqlancer.common.oracle.NoRECBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;

public class ClickHouseNoRECOracle extends NoRECBase<ClickHouseGlobalState> implements TestOracle {

    private final ClickHouseSchema s;

    public ClickHouseNoRECOracle(ClickHouseGlobalState globalState) {
        super(globalState);
        this.s = globalState.getSchema();
        ClickHouseErrors.addExpectedExpressionErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        ClickHouseTables randomTables = s.getRandomTableNonEmptyTables();
        List<ClickHouseColumn> columns = randomTables.getColumns();
        ClickHouseExpressionGenerator gen = new ClickHouseExpressionGenerator(state).setColumns(columns);
        ClickHouseExpression randomWhereCondition = gen.generateExpression(ClickHouseLancerDataType.getRandom());
        List<ClickHouseTable> tables = randomTables.getTables();
        List<ClickHouseTableReference> tableList = tables.stream().map(t -> new ClickHouseTableReference(t))
                .collect(Collectors.toList());
        List<ClickHouseExpression.ClickHouseJoin> joins = gen.getRandomJoinClauses(tables);
        int secondCount = getSecondQuery(tableList.stream().collect(Collectors.toList()), randomWhereCondition, joins);
        int firstCount = getFirstQueryCount(con, tableList.stream().collect(Collectors.toList()), columns,
                randomWhereCondition, joins);
        if (firstCount == -1 || secondCount == -1) {
            throw new IgnoreMeException();
        }
        if (firstCount != secondCount) {
            throw new AssertionError(
                    optimizedQueryString + "; -- " + firstCount + "\n" + unoptimizedQueryString + " -- " + secondCount);
        }
    }

    private int getSecondQuery(List<ClickHouseExpression> tableList, ClickHouseExpression whereClause,
            List<ClickHouseExpression.ClickHouseJoin> joins) throws SQLException {
        ClickHouseSelect select = new ClickHouseSelect();

        ClickHouseExpression inner = new ClickHouseAliasOperation(whereClause, "check");

        select.setFetchColumns(Arrays.asList(inner));
        select.setFromList(tableList);
        select.setJoinClauses(joins);
        int secondCount = 0;
        unoptimizedQueryString = "SELECT SUM(check <> 0) FROM (" + ClickHouseToStringVisitor.asString(select)
                + ") as res";
        errors.add("canceling statement due to statement timeout");
        SQLQueryAdapter q = new SQLQueryAdapter(unoptimizedQueryString, errors);
        SQLancerResultSet rs;
        try {
            rs = q.executeAndGetLogged(state);
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

    private int getFirstQueryCount(SQLConnection con, List<ClickHouseExpression> tableList,
            List<ClickHouseColumn> columns, ClickHouseExpression randomWhereCondition,
            List<ClickHouseExpression.ClickHouseJoin> joins) throws SQLException {
        ClickHouseSelect select = new ClickHouseSelect();
        List<ClickHouseExpression> allColumns = columns.stream().map((c) -> new ClickHouseColumnReference(c))
                .collect(Collectors.toList());
        select.setFetchColumns(allColumns);
        select.setFromList(tableList);
        select.setWhereClause(randomWhereCondition);
        if (Randomly.getBooleanWithSmallProbability()) {
            select.setOrderByExpressions(
                    new ClickHouseExpressionGenerator(state).setColumns(columns).generateOrderBys());
        }
        select.setJoinClauses(joins);
        int firstCount = 0;
        try (Statement stat = con.createStatement()) {
            optimizedQueryString = ClickHouseToStringVisitor.asString(select);
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
