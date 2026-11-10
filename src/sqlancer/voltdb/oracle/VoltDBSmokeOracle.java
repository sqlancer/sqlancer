package sqlancer.voltdb.oracle;

import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.voltdb.VoltDBProvider.VoltDBGlobalState;
import sqlancer.voltdb.VoltDBSchema.VoltDBTable;

/**
 * Counts and reads the rows of every table, checking only that the queries succeed. It has no notion of a correct
 * result; it exists so that schema and data generation can run on their own.
 */
public class VoltDBSmokeOracle implements TestOracle<VoltDBGlobalState> {

    private final VoltDBGlobalState state;
    private String lastQuery;

    public VoltDBSmokeOracle(VoltDBGlobalState state) {
        this.state = state;
    }

    @Override
    public void check() throws Exception {
        for (VoltDBTable t : state.getSchema().getDatabaseTables()) {
            read("SELECT COUNT(*) FROM " + t.getName());
            read("SELECT * FROM " + t.getName());
        }
    }

    private void read(String sql) throws Exception {
        lastQuery = sql;
        SQLQueryAdapter q = new SQLQueryAdapter(sql, ExpectedErrors.newErrors().build());
        long rows = 0;
        try (SQLancerResultSet rs = q.executeAndGet(state)) {
            while (rs != null && rs.next()) {
                rows++; // the rows themselves are not checked
            }
        }
        state.getLogger().writeCurrent("-- " + rows + " rows");
    }

    @Override
    public String getLastQueryString() {
        return lastQuery;
    }

}
