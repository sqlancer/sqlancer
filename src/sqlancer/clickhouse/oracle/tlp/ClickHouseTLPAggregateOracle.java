package sqlancer.clickhouse.oracle.tlp;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import sqlancer.ComparatorHelper;
import sqlancer.Randomly;
import sqlancer.clickhouse.ClickHouseErrors;
import sqlancer.clickhouse.ClickHouseProvider;
import sqlancer.clickhouse.ClickHouseVisitor;
import sqlancer.clickhouse.ast.ClickHouseAggregate;
import sqlancer.clickhouse.ast.ClickHouseAliasOperation;

public class ClickHouseTLPAggregateOracle extends ClickHouseTLPBase {

    public ClickHouseTLPAggregateOracle(ClickHouseProvider.ClickHouseGlobalState state) {
        super(state);
        ClickHouseErrors.addExpectedExpressionErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        super.check();
        if (Randomly.getBooleanWithRatherLowProbability()) {
            select.setOrderByExpressions(IntStream.range(0, 1 + Randomly.smallNumber())
                    .mapToObj(i -> gen.generateExpressionWithColumns(columns, 5)).collect(Collectors.toList()));
        }

        ClickHouseAggregate.ClickHouseAggregateFunction windowFunction = Randomly.fromOptions(
                ClickHouseAggregate.ClickHouseAggregateFunction.MIN,
                ClickHouseAggregate.ClickHouseAggregateFunction.MAX,
                ClickHouseAggregate.ClickHouseAggregateFunction.SUM);

        ClickHouseAggregate aggregate = new ClickHouseAggregate(gen.generateExpressionWithColumns(columns, 6),
                windowFunction);
        select.setFetchColumns(Arrays.asList(aggregate));

        String originalQuery = ClickHouseVisitor.asString(select);
        originalQuery += " SETTINGS aggregate_functions_null_for_empty = 1";

        select.setFetchColumns(Arrays.asList(new ClickHouseAliasOperation(aggregate, "aggr")));

        select.setWhereClause(predicate);
        if (Randomly.getBooleanWithRatherLowProbability()) {
            select.setGroupByClause(IntStream.range(0, 1 + Randomly.smallNumber())
                    .mapToObj(i -> gen.generateExpressionWithColumns(columns, 5)).collect(Collectors.toList()));
        }
        if (Randomly.getBoolean()) {
            select.setOrderByExpressions(IntStream.range(0, 1 + Randomly.smallNumber())
                    .mapToObj(i -> gen.generateExpressionWithColumns(columns, 5)).collect(Collectors.toList()));
        }

        String metamorphicText = "SELECT " + aggregate.getFunc().toString() + "(aggr) FROM (";
        metamorphicText += ClickHouseVisitor.asString(select) + " UNION ALL ";
        select.setWhereClause(negatedPredicate);
        metamorphicText += ClickHouseVisitor.asString(select) + " UNION ALL ";
        select.setWhereClause(isNullPredicate);
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
            return;
        } else if (firstResult.size() == 1 && secondResult.size() == 1) {
            if (firstResult.get(0).equals(secondResult.get(0))) {
                return;
            } else if (!ComparatorHelper.isEqualDouble(firstResult.get(0), secondResult.get(0))) {
                throw new AssertionError();
            }
        } else {
            throw new AssertionError();
        }
    }

}
