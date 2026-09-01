package sqlancer.clickhouse.oracle.tlp;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import sqlancer.ComparatorHelper;
import sqlancer.clickhouse.ClickHouseErrors;
import sqlancer.clickhouse.ClickHouseProvider.ClickHouseGlobalState;
import sqlancer.clickhouse.ClickHouseVisitor;

public class ClickHouseTLPWhereOracle extends ClickHouseTLPBase {

    public ClickHouseTLPWhereOracle(ClickHouseGlobalState state) {
        super(state);
        ClickHouseErrors.addExpectedExpressionErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        super.check();

        select.setWhereClause(null);
        String originalQueryString = ClickHouseVisitor.asString(select);
        originalQueryString += " SETTINGS join_use_nulls = 1, enable_optimize_predicate_expression = 0, aggregate_functions_null_for_empty = 1";

        List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);

        select.setWhereClause(predicate);
        String firstQueryString = ClickHouseVisitor.asString(select);
        select.setWhereClause(negatedPredicate);
        String secondQueryString = ClickHouseVisitor.asString(select);
        select.setWhereClause(isNullPredicate);
        String thirdQueryString = ClickHouseVisitor.asString(select);
        List<String> combinedString = new ArrayList<>();
        List<String> secondResultSet = ComparatorHelper.getCombinedResultSet(firstQueryString
                + " SETTINGS join_use_nulls = 1, enable_optimize_predicate_expression = 0, aggregate_functions_null_for_empty = 1",
                secondQueryString
                        + " SETTINGS join_use_nulls = 1, enable_optimize_predicate_expression = 0, aggregate_functions_null_for_empty = 1",
                thirdQueryString
                        + " SETTINGS join_use_nulls = 1, enable_optimize_predicate_expression = 0, aggregate_functions_null_for_empty = 1",
                combinedString, true, state, errors);
        ComparatorHelper.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQueryString, combinedString,
                state);
    }
}
