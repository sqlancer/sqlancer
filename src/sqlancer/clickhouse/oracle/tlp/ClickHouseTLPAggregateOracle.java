package sqlancer.clickhouse.oracle.tlp;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import ru.yandex.clickhouse.domain.ClickHouseDataType;
import sqlancer.ComparatorHelper;
import sqlancer.Randomly;
import sqlancer.clickhouse.ClickHouseErrors;
import sqlancer.clickhouse.ClickHouseProvider;
import sqlancer.clickhouse.ClickHouseSchema;
import sqlancer.clickhouse.ClickHouseVisitor;
import sqlancer.clickhouse.ast.ClickHouseAggregate;
import sqlancer.clickhouse.ast.ClickHouseExpression;
import sqlancer.clickhouse.ast.ClickHouseSelect;
import sqlancer.clickhouse.ast.ClickHouseUnaryPostfixOperation;
import sqlancer.clickhouse.ast.ClickHouseUnaryPrefixOperation;
import sqlancer.clickhouse.gen.ClickHouseCommon;
import sqlancer.clickhouse.gen.ClickHouseExpressionGenerator;

public class ClickHouseTLPAggregateOracle extends ClickHouseTLPBase {

    private ClickHouseExpressionGenerator gen;

    public ClickHouseTLPAggregateOracle(ClickHouseProvider.ClickHouseGlobalState state) {
        super(state);
        ClickHouseErrors.addExpectedExpressionErrors(errors);
        ClickHouseErrors.addQueryErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        ClickHouseSchema s = state.getSchema();
        ClickHouseSchema.ClickHouseTables targetTables = s.getRandomTableNonEmptyTables();
        gen = new ClickHouseExpressionGenerator(state).setColumns(targetTables.getColumns());
        ClickHouseSelect select = new ClickHouseSelect();
        ClickHouseAggregate.ClickHouseAggregateFunction windowFunction = Randomly.fromOptions(
                ClickHouseAggregate.ClickHouseAggregateFunction.MIN,
                ClickHouseAggregate.ClickHouseAggregateFunction.MAX,
                ClickHouseAggregate.ClickHouseAggregateFunction.SUM);
        ClickHouseAggregate aggregate = new ClickHouseAggregate(
                gen.generateExpressions(ClickHouseSchema.ClickHouseLancerDataType.getRandom(), 1), windowFunction);
        select.setFetchColumns(Arrays.asList(aggregate));
        List<ClickHouseExpression> from = ClickHouseCommon.getTableRefs(targetTables.getTables(), s);
        select.setFromList(from);
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

        ClickHouseSelect leftSelect = getSelect(aggregate, from, whereClause);
        ClickHouseSelect middleSelect = getSelect(aggregate, from, negatedClause);
        ClickHouseSelect rightSelect = getSelect(aggregate, from, notNullClause);
        String metamorphicText = "SELECT " + aggregate.getFunc().toString() + "(aggr) FROM (";
        metamorphicText += ClickHouseVisitor.asString(leftSelect) + " UNION ALL "
                + ClickHouseVisitor.asString(middleSelect) + " UNION ALL " + ClickHouseVisitor.asString(rightSelect);
        metamorphicText += ")";
        metamorphicText += " SETTINGS aggregate_functions_null_for_empty = 1";
        List<String> firstResult = ComparatorHelper.getResultSetFirstColumnAsString(originalQuery, errors, state);

        List<String> secondResult = ComparatorHelper.getResultSetFirstColumnAsString(metamorphicText, errors, state);

        state.getState().getLocalState()
                .log("--" + originalQuery + "\n--" + metamorphicText + "\n-- " + firstResult + "\n-- " + secondResult
                        + "\n--first size " + firstResult.size() + "\n--second size " + secondResult.size());

        if (firstResult.size() != secondResult.size()) {
            throw new AssertionError();
        } else if (firstResult.isEmpty()) {
            return;
        } else if (firstResult.size() == 1) {
            if (firstResult.get(0).equals(secondResult.get(0))) {
                return;
            } else if (!ComparatorHelper.isEqualDouble(firstResult.get(0), secondResult.get(0))) {
                throw new AssertionError();
            }
        } else {
            throw new AssertionError();
        }
    }

    private ClickHouseSelect getSelect(ClickHouseAggregate aggregate, List<ClickHouseExpression> from,
            ClickHouseExpression whereClause) {
        ClickHouseSelect leftSelect = new ClickHouseSelect();
        leftSelect.setFetchColumns(
                Arrays.asList(new ClickHouseExpression.ClickHousePostfixText(aggregate, " as aggr", null)));
        leftSelect.setFromList(from);
        leftSelect.setWhereClause(whereClause);
        if (Randomly.getBooleanWithRatherLowProbability()) {
            leftSelect.setGroupByClause(gen.generateExpressions(Randomly.smallNumber() + 1));
        }
        if (Randomly.getBoolean()) {
            leftSelect.setOrderByExpressions(gen.generateOrderBys());
        }
        return leftSelect;
    }

}
