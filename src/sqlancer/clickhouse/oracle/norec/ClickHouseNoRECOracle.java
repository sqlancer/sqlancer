package sqlancer.clickhouse.oracle.norec;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.clickhouse.ClickHouseErrors;
import sqlancer.clickhouse.ClickHouseProvider.ClickHouseGlobalState;
import sqlancer.clickhouse.ClickHouseSchema;
import sqlancer.clickhouse.ClickHouseSchema.ClickHouseTable;
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

public class ClickHouseNoRECOracle extends NoRECBase<ClickHouseGlobalState>
        implements TestOracle<ClickHouseGlobalState> {

    private final ClickHouseSchema schema;

    public ClickHouseNoRECOracle(ClickHouseGlobalState globalState) {
        super(globalState);
        this.schema = globalState.getSchema();
        ClickHouseErrors.addExpectedExpressionErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        ClickHouseExpressionGenerator gen = new ClickHouseExpressionGenerator(state);
        List<ClickHouseTable> tables = schema.getRandomTableNonEmptyTables().getTables();
        ClickHouseTableReference table = new ClickHouseTableReference(
                tables.get((int) Randomly.getNotCachedInteger(0, tables.size() - 1)), "left");
        List<ClickHouseColumnReference> columns = table.getColumnReferences();

        List<ClickHouseExpression.ClickHouseJoin> joinStatements = new ArrayList<>();
        if (state.getClickHouseOptions().testJoins && Randomly.getBoolean()) {
            joinStatements = gen.getRandomJoinClauses(table, tables);
            columns.addAll(joinStatements.stream().flatMap(j -> j.getRightTable().getColumnReferences().stream())
                    .collect(Collectors.toList()));
        }
        gen.addColumns(columns);

        ClickHouseExpression randomWhereCondition = gen.generateExpressionWithColumns(columns, 5);
        int secondCount = getSecondQuery(table, randomWhereCondition, joinStatements);
        int firstCount = getFirstQueryCount(table, columns, randomWhereCondition, joinStatements);
        if (firstCount == -1 || secondCount == -1) {
            throw new IgnoreMeException();
        }
        if (firstCount != secondCount) {
            throw new AssertionError(
                    optimizedQueryString + "; -- " + firstCount + "\n" + unoptimizedQueryString + " -- " + secondCount);
        }
    }

    private int getSecondQuery(ClickHouseExpression table, ClickHouseExpression whereClause,
            List<ClickHouseExpression.ClickHouseJoin> joins) throws SQLException {
        ClickHouseSelect select = new ClickHouseSelect();

        ClickHouseExpression inner = new ClickHouseAliasOperation(whereClause, "check");

        select.setFetchColumns(Arrays.asList(inner));
        select.setFromClause(table);
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

    private int getFirstQueryCount(ClickHouseExpression tableList, List<ClickHouseColumnReference> columns,
            ClickHouseExpression randomWhereCondition, List<ClickHouseExpression.ClickHouseJoin> joins)
            throws SQLException {
        ClickHouseSelect select = new ClickHouseSelect();
        List<ClickHouseColumnReference> filteredColumns = Randomly.extractNrRandomColumns(columns,
                (int) Randomly.getNotCachedInteger(1, columns.size()));
        select.setFetchColumns(
                filteredColumns.stream().map(c -> (ClickHouseExpression) c).collect(Collectors.toList()));
        select.setFromClause(tableList);
        select.setWhereClause(randomWhereCondition);
        select.setJoinClauses(joins);
        int firstCount = 0;
        optimizedQueryString = ClickHouseToStringVisitor.asString(select);
        SQLQueryAdapter q = new SQLQueryAdapter(optimizedQueryString, errors);
        SQLancerResultSet rs;

        try {
            rs = q.executeAndGetLogged(state);
        } catch (Exception e) {
            throw new AssertionError(optimizedQueryString, e);
        }
        if (rs == null) {
            return -1;
        }

        while (rs.next()) {
            firstCount++;
        }
        rs.close();
        return firstCount;
    }

}
