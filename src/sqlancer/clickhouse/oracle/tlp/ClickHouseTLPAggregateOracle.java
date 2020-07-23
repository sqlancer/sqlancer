package sqlancer.clickhouse.oracle.tlp;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import ru.yandex.clickhouse.domain.ClickHouseDataType;
import sqlancer.ComparatorHelper;
import sqlancer.IgnoreMeException;
import sqlancer.QueryAdapter;
import sqlancer.Randomly;
import sqlancer.TestOracle;
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

public class ClickHouseTLPAggregateOracle implements TestOracle {

    private final ClickHouseProvider.ClickHouseGlobalState state;
    private ClickHouseExpressionGenerator gen;

    public ClickHouseTLPAggregateOracle(ClickHouseProvider.ClickHouseGlobalState state) {
        this.state = state;
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

        String firstResult;
        String secondResult;
        QueryAdapter q = new QueryAdapter(originalQuery);
        try (ResultSet result = q.executeAndGet(state)) {
            if (result == null) {
                throw new IgnoreMeException();
            }
            firstResult = result.getString(1);
        } catch (Exception e) {
            // TODO
            throw new IgnoreMeException();
        }

        QueryAdapter q2 = new QueryAdapter(metamorphicText);
        try (ResultSet result = q2.executeAndGet(state)) {
            if (result == null) {
                throw new IgnoreMeException();
            }
            secondResult = result.getString(1);
        } catch (Exception e) {
            // TODO
            throw new IgnoreMeException();
        }
        state.getState().queryString = "--" + originalQuery + "\n--" + metamorphicText + "\n-- " + firstResult + "\n-- "
                + secondResult;
        if ((firstResult == null && secondResult != null
                || firstResult != null && !firstResult.contentEquals(secondResult))
                && !ComparatorHelper.isEqualDouble(firstResult, secondResult)) {

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
