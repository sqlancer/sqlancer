package sqlancer.voltdb.oracle;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import sqlancer.IgnoreMeException;
import sqlancer.Reproducer;
import sqlancer.common.oracle.TestOracle;
import sqlancer.voltdb.VoltDBConnection;
import sqlancer.voltdb.VoltDBProvider.VoltDBGlobalState;
import sqlancer.voltdb.VoltDBSchema.VoltDBTable;

/**
 * XDCR convergence (run-sqlancer.sh --xdcr). The database writes through one cluster of an XDCR pair and runs its DDL
 * on both, so once replication has caught up, every table and materialized view must hold the same rows on both
 * clusters. XDCR compares them as multisets, retrying for up to {@link #CONVERGE_MILLIS} while replication catches up,
 * and fails with the rows found on one side only. A view's rows on the other side come from rows that XDCR applied, so
 * this also checks view maintenance on that path.
 *
 * <p>
 * The database's thread is its only writer, and it is in this check, so nothing changes the rows while they are read. A
 * database whose schema change one cluster refused is skipped, as their tables may differ. A failure has no reproducer:
 * it depends on replication, which a reduction cannot repeat.
 */
public class VoltDBXdcrOracle implements TestOracle<VoltDBGlobalState> {

    private static final long CONVERGE_MILLIS = 60_000;
    private static final String ERROR = "error: ";

    private final VoltDBGlobalState state;
    private boolean reported;
    private String lastQuery;

    public VoltDBXdcrOracle(VoltDBGlobalState state) {
        this.state = state;
    }

    @Override
    public void check() throws Exception {
        VoltDBConnection connection = (VoltDBConnection) state.getConnection();
        VoltDBConnection peer = connection.getPeer();
        if (peer == null || connection.isPeerSplit()) {
            throw new IgnoreMeException();
        }
        List<String> names = new ArrayList<>();
        for (VoltDBTable table : state.getSchema().getDatabaseTables()) {
            names.add(table.getName());
        }
        names.addAll(state.getViews().keySet());
        long deadline = System.currentTimeMillis() + CONVERGE_MILLIS;
        List<String> differences;
        while (true) {
            differences = new ArrayList<>();
            for (String name : names) {
                String sql = "SELECT * FROM " + name;
                lastQuery = sql;
                List<String> here = rows(connection, sql);
                List<String> there = rows(peer, sql);
                // a table neither side has (the provider's schema can be behind) is not a difference
                boolean bothFailed = here.size() == 1 && here.get(0).startsWith(ERROR) && there.size() == 1
                        && there.get(0).startsWith(ERROR);
                if (!here.equals(there) && !bothFailed) {
                    differences.add(name + ": " + here.size() + " rows here, " + there.size()
                            + " on the other cluster; only here: " + VoltDBRows.difference(here, there)
                            + "; only there: " + VoltDBRows.difference(there, here));
                }
            }
            if (connection.isPeerSplit()) {
                throw new IgnoreMeException();
            }
            if (differences.isEmpty() || System.currentTimeMillis() >= deadline) {
                break;
            }
            Thread.sleep(1000);
        }
        if (!differences.isEmpty()) {
            throw new AssertionError("the XDCR clusters did not converge within " + CONVERGE_MILLIS / 1000 + " s: "
                    + String.join(" | ", differences));
        }
        // run-sqlancer.sh counts these
        if (!reported) {
            reported = true;
            System.out
                    .println("XDCR converged: " + state.getDatabaseName() + ", " + names.size() + " tables and views");
        }
    }

    // the rows, each its values joined with " | ", sorted; or the one error that stopped the query
    private static List<String> rows(VoltDBConnection connection, String sql) {
        List<String> rows = new ArrayList<>();
        try (Statement st = connection.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            int columns = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                List<String> row = new ArrayList<>();
                for (int i = 1; i <= columns; i++) {
                    Object v = rs.getObject(i);
                    row.add(v instanceof byte[] ? java.util.Arrays.toString((byte[]) v) : String.valueOf(v));
                }
                rows.add(String.join(" | ", row));
            }
        } catch (SQLException e) {
            return List.of(ERROR + e.getMessage());
        }
        Collections.sort(rows);
        return rows;
    }

    @Override
    public Reproducer<VoltDBGlobalState> getLastReproducer() {
        return null;
    }

    @Override
    public String getLastQueryString() {
        return lastQuery;
    }

}
