package sqlancer.clickhouse.oracle.tlp;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import com.clickhouse.client.ClickHouseDataType;

import sqlancer.ComparatorHelper;
import sqlancer.Randomly;
import sqlancer.clickhouse.ClickHouseErrors;
import sqlancer.clickhouse.ClickHouseProvider;
import sqlancer.clickhouse.ClickHouseSchema;
import sqlancer.clickhouse.ClickHouseVisitor;
import sqlancer.clickhouse.ast.ClickHouseAggregate;
import sqlancer.clickhouse.ast.ClickHouseAliasOperation;
import sqlancer.clickhouse.ast.ClickHouseExpression;
import sqlancer.clickhouse.ast.ClickHouseSelect;
import sqlancer.clickhouse.ast.ClickHouseTableReference;
import sqlancer.clickhouse.ast.ClickHouseUnaryPostfixOperation;
import sqlancer.clickhouse.ast.ClickHouseUnaryPrefixOperation;
import sqlancer.clickhouse.gen.ClickHouseExpressionGenerator;

public class ClickHouseTLPAggregateOracle extends ClickHouseTLPBase {

    public ClickHouseTLPAggregateOracle(ClickHouseProvider.ClickHouseGlobalState state) {
        super(state);
        ClickHouseErrors.addExpectedExpressionErrors(errors);
    }

    @Override
    public String check() throws SQLException {
        ClickHouseSchema s = state.getSchema();
        ClickHouseSchema.ClickHouseTables randomTables = s.getRandomTableNonEmptyTables();
        ClickHouseSchema.ClickHouseTable table = randomTables.getTables().remove(0);
        ClickHouseTableReference tableRef = new ClickHouseTableReference(table, table.getName());
        List<ClickHouseSchema.ClickHouseColumn> columns = randomTables.getColumns();
        columns.addAll(table.getColumns());
        ClickHouseExpressionGenerator gen = new ClickHouseExpressionGenerator(state).setColumns(columns);
        List<ClickHouseExpression.ClickHouseJoin> joins = gen.getRandomJoinClauses(tableRef, randomTables.getTables());

        gen = new ClickHouseExpressionGenerator(state).setColumns(randomTables.getColumns());
        ClickHouseSelect select = new ClickHouseSelect();
        ClickHouseAggregate.ClickHouseAggregateFunction windowFunction = Randomly.fromOptions(
                ClickHouseAggregate.ClickHouseAggregateFunction.MIN,
                ClickHouseAggregate.ClickHouseAggregateFunction.MAX,
                ClickHouseAggregate.ClickHouseAggregateFunction.SUM);
        ClickHouseAggregate aggregate = new ClickHouseAggregate(
                gen.generateExpressions(ClickHouseSchema.ClickHouseLancerDataType.getRandom(), 1), windowFunction);
        select.setFetchColumns(Arrays.asList(aggregate));
        ClickHouseExpression from = tableRef;
        select.setFromClause(from);
        select.setJoinClauses(joins);
        if (Randomly.getBoolean()) {
            select.setOrderByExpressions(gen.generateOrderBys());
        }
        String originalQuery = ClickHouseVisitor.asString(select);
        originalQuery += " SETTINGS aggregate_functions_null_for_empty = 1";

        ClickHouseExpression whereClause = gen
                .generateExpression(new ClickHouseSchema.ClickHouseLancerDataType(ClickHouseDataType.UInt8));
        ClickHouseUnaryPrefixOperation negatedClause = new ClickHouseUnaryPrefixOperation(whereClause,
                ClickHouseUnaryPrefixOperation.ClickHouseUnaryPrefixOperator.NOT);
        ClickHouseUnaryPostfixOperation notNullClause = new ClickHouseUnaryPostfixOperation(whereClause,
                ClickHouseUnaryPostfixOperation.ClickHouseUnaryPostfixOperator.IS_NULL, false);

        select.setFetchColumns(Arrays.asList(new ClickHouseAliasOperation(aggregate, "aggr")));
        select.setFromClause(from);
        select.setWhereClause(whereClause);
        if (Randomly.getBooleanWithRatherLowProbability()) {
            select.setGroupByClause(gen.generateExpressions(Randomly.smallNumber() + 1));
        }
        if (Randomly.getBoolean()) {
            select.setOrderByExpressions(gen.generateOrderBys());
        }

        String metamorphicText = "SELECT " + aggregate.getFunc().toString() + "(aggr) FROM (";
        metamorphicText += ClickHouseVisitor.asString(select) + " UNION ALL ";
        select.setWhereClause(negatedClause);
        metamorphicText += ClickHouseVisitor.asString(select) + " UNION ALL ";
        select.setWhereClause(notNullClause);
        metamorphicText += ClickHouseVisitor.asString(select);
        metamorphicText += ")";
        metamorphicText += " SETTINGS aggregate_functions_null_for_empty = 1";
        List<String> firstResult = ComparatorHelper.getResultSetFirstColumnAsString(originalQuery, errors, state);

        List<String> secondResult = ComparatorHelper.getResultSetFirstColumnAsString(metamorphicText, errors, state);

        state.getState().getLocalState()
                .log("--" + originalQuery + "\n--" + metamorphicText + "\n-- " + firstResult + "\n-- " + secondResult
                        + "\n--first size " + firstResult.size() + "\n--second size " + secondResult.size());

        if (firstResult.size() != secondResult.size()) {
            throw new AssertionError();
        } else if (firstResult.isEmpty() || firstResult.equals(secondResult)) {
            return "Not implemented!";
        } else if (firstResult.size() == 1 && secondResult.size() == 1) {
            if (firstResult.get(0).equals(secondResult.get(0))) {
                return "Not implemented!";
            } else if (!ComparatorHelper.isEqualDouble(firstResult.get(0), secondResult.get(0))) {
                throw new AssertionError();
            }
        } else {
            throw new AssertionError();
        }
        return "Not implemented!";
    }

}
