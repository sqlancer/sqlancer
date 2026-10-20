package sqlancer.cloudberry.oracle.tlp;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import sqlancer.ComparatorHelper;
import sqlancer.IgnoreMeException;
import sqlancer.cloudberry.CloudberryBugs;
import sqlancer.cloudberry.gen.CloudberryCommon;
import sqlancer.cloudberry.oracle.CloudberryOracleUtils;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresSchema.PostgresColumn;
import sqlancer.postgres.PostgresVisitor;
import sqlancer.postgres.ast.PostgresColumnValue;
import sqlancer.postgres.ast.PostgresExpression;
import sqlancer.postgres.gen.PostgresCommon;
import sqlancer.postgres.oracle.tlp.PostgresTLPBase;

// For queries that remove duplicates (DISTINCT, GROUP BY), the union of the three partitions has to remove
// duplicates again.
public abstract class CloudberryTLPDeduplicationBase extends PostgresTLPBase {

    protected CloudberryTLPDeduplicationBase(PostgresGlobalState state) {
        super(state);
        PostgresCommon.addGroupingErrors(errors);
        CloudberryCommon.addCloudberryErrors(errors);
    }

    protected abstract void prepareSelect();

    @Override
    public void check() throws SQLException {
        // Each query is evaluated several times, so functions that are not immutable could change the result.
        state.setAllowedFunctionTypes(Arrays.asList(PostgresGlobalState.IMMUTABLE));
        try {
            super.check();
            // FOR UPDATE / FOR SHARE is not allowed with DISTINCT or GROUP BY.
            select.setForClause(null);
            CloudberryOracleUtils.removeOnly(select);
            prepareSelect();
            if (CloudberryBugs.bug1941 && hasUniqueColumn()) {
                throw new IgnoreMeException();
            }
            int columnCount = select.getFetchColumns().size();

            select.setWhereClause(null);
            String originalQueryString = CloudberryOracleUtils.wrap(PostgresVisitor.asString(select), columnCount);
            List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors,
                    state);

            select.setWhereClause(predicate);
            String firstQueryString = PostgresVisitor.asString(select);
            select.setWhereClause(negatedPredicate);
            String secondQueryString = PostgresVisitor.asString(select);
            select.setWhereClause(isNullPredicate);
            String thirdQueryString = PostgresVisitor.asString(select);
            String combinedQueryString = CloudberryOracleUtils.wrap("SELECT DISTINCT * FROM (" + firstQueryString
                    + " UNION ALL " + secondQueryString + " UNION ALL " + thirdQueryString + ") AS u", columnCount);
            List<String> secondResultSet = ComparatorHelper.getResultSetFirstColumnAsString(combinedQueryString, errors,
                    state);
            ComparatorHelper.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQueryString,
                    Arrays.asList(combinedQueryString), state);
        } finally {
            state.setDefaultAllowedFunctionTypes();
        }
    }

    private boolean hasUniqueColumn() throws SQLException {
        for (PostgresExpression fetchColumn : select.getFetchColumns()) {
            PostgresColumn column = ((PostgresColumnValue) fetchColumn).getColumn();
            String queryString = "SELECT 1 FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid "
                    + "AND a.attnum = ANY(i.indkey) WHERE i.indisunique AND i.indrelid = '"
                    + column.getTable().getName() + "'::regclass AND a.attname = '" + column.getName() + "'";
            SQLQueryAdapter query = new SQLQueryAdapter(queryString, ExpectedErrors.from("does not exist"));
            try (SQLancerResultSet rs = query.executeAndGet(state)) {
                if (rs == null || rs.next()) {
                    return true;
                }
            }
        }
        return false;
    }

}
