package sqlancer.cloudberry.oracle;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import sqlancer.ComparatorHelper;
import sqlancer.Randomly;
import sqlancer.cloudberry.gen.CloudberryCommon;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresSchema.PostgresDataType;
import sqlancer.postgres.PostgresVisitor;
import sqlancer.postgres.oracle.tlp.PostgresTLPBase;

public class CloudberryDQPOracle extends PostgresTLPBase {

    // Settings that lead to different plans. Cloudberry disables merge and nested loop joins in the PostgreSQL-based
    // planner by default.
    private static final List<List<String>> PLAN_VARIANTS = Arrays.asList(Arrays.asList("optimizer = off"),
            Arrays.asList("optimizer = off", "enable_hashjoin = off"),
            Arrays.asList("optimizer = off", "enable_mergejoin = on"),
            Arrays.asList("optimizer = off", "enable_nestloop = on"),
            Arrays.asList("optimizer = off", "enable_hashagg = off"),
            Arrays.asList("optimizer = off", "enable_seqscan = off"),
            Arrays.asList("optimizer = off", "gp_enable_multiphase_agg = off"),
            Arrays.asList("optimizer_enable_hashjoin = off"), Arrays.asList("optimizer_enable_mergejoin = off"),
            Arrays.asList("optimizer_enable_nljoin = off"), Arrays.asList("optimizer_enable_indexscan = off"),
            Arrays.asList("optimizer_enable_hashagg = off"),
            Arrays.asList("optimizer_enable_motion_redistribute = off"),
            Arrays.asList("optimizer_enable_motion_broadcast = off"));

    // Not every Cloudberry version has every setting.
    private static final ExpectedErrors SET_ERRORS = ExpectedErrors.from("unrecognized configuration parameter");

    public CloudberryDQPOracle(PostgresGlobalState state) {
        super(state);
        CloudberryCommon.addCloudberryErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        // Functions that are not immutable can return different values in two executions of the same query.
        state.setAllowedFunctionTypes(Arrays.asList(PostgresGlobalState.IMMUTABLE));
        try {
            super.check();
            select.setForClause(null);
            CloudberryOracleUtils.removeOnly(select);
            select.setFetchColumns(CloudberryOracleUtils.randomFetchColumns(targetTables.getColumns()));
            if (Randomly.getBoolean()) {
                select.setWhereClause(gen.generateExpression(PostgresDataType.BOOLEAN));
            }
            String queryString = CloudberryOracleUtils.wrap(PostgresVisitor.asString(select),
                    select.getFetchColumns().size());
            List<String> expectedResult = ComparatorHelper.getResultSetFirstColumnAsString(queryString, errors, state);
            for (List<String> variant : PLAN_VARIANTS) {
                checkVariant(queryString, expectedResult, variant);
            }
        } finally {
            state.setDefaultAllowedFunctionTypes();
        }
    }

    private void checkVariant(String queryString, List<String> expectedResult, List<String> settings)
            throws SQLException {
        List<String> statements = new ArrayList<>();
        try {
            for (String setting : settings) {
                if (!execute("SET " + setting, SET_ERRORS)) {
                    return;
                }
                statements.add("SET " + setting);
            }
            statements.add(queryString);
            List<String> result = ComparatorHelper.getResultSetFirstColumnAsString(queryString, errors, state);
            ComparatorHelper.assumeResultSetsAreEqual(expectedResult, result, queryString, statements, state);
        } finally {
            for (String setting : settings) {
                execute("RESET " + setting.split(" ")[0], SET_ERRORS);
            }
        }
    }

    private boolean execute(String sql, ExpectedErrors expectedErrors) throws SQLException {
        if (state.getOptions().logEachSelect()) {
            state.getLogger().writeCurrent(sql);
        }
        return new SQLQueryAdapter(sql, expectedErrors).execute(state);
    }

}
