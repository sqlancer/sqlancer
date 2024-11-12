package sqlancer.influxdb.test;

import java.sql.SQLException;

import sqlancer.influxdb.errors.InfluxDBErrors;

public class InfluxDBQueryPartitioningWhereTester extends InfluxDBQueryPartitioningBase {

    public InfluxDBQueryPartitioningWhereTester(InfluxDBGlobalState state) {
        super(state);
        InfluxDBErrors.addGroupByErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        super.check();
        select.setWhereClause(null);
        String originalQueryString = InfluxDBToStringVisitor.asString(select);

        List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);

        // Ignore OrderBy for now

        select.setWhereClause(predicate);
        String firstQueryString = InfluxDBToStringVisitor.asString(select);
        select.setWhereClause(negatedPredicate);
        String secondQueryString = InfluxDBToStringVisitor.asString(select);
        select.setWhereClause(isNullPredicate);
        String thirdQueryString = InfluxDBToStringVisitor.asString(select);

        List<String> combinedString = new ArrayList<>();
        List<String> secondResultSet = ComparatorHelper.getCombinedResultSet(firstQueryString, secondQueryString,
                thirdQueryString, combinedString, false, state, errors);
        ComparatorHelper.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQueryString, combinedString,
                state, ComparatorHelper::canonicalizeResultValue);
    }
}