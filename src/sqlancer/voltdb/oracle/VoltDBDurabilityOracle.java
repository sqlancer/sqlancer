package sqlancer.voltdb.oracle;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import sqlancer.IgnoreMeException;
import sqlancer.Reproducer;
import sqlancer.common.oracle.TestOracle;
import sqlancer.voltdb.VoltDBConnection;
import sqlancer.voltdb.VoltDBProvider.VoltDBGlobalState;
import sqlancer.voltdb.VoltDBSchema.VoltDBTable;

/**
 * Durability across whole-cluster crashes (run-sqlancer.sh --crash-interval). With {@code --mirror}, every change also
 * runs on a second cluster that never crashes, so after a crash and the recovery from snapshots and the synchronous
 * command log, every table must still hold as many rows on this cluster as on the mirror: a change VoltDB acknowledged
 * must survive. DURABLE counts each of the database's tables on both clusters and compares; a table missing here but
 * present on the mirror is a lost table.
 *
 * <p>
 * The counts are cheap, so a check after each crash is likely. DIFF compares the rows themselves, but only on tables
 * that are not empty, and so it cannot see a table that recovery left empty. A database whose change was in flight at a
 * crash may differ (the change may or may not have committed), and is skipped, as is every check without a mirror. A
 * failure has no reproducer: it depends on the crash, which a reduction cannot repeat.
 */
public class VoltDBDurabilityOracle implements TestOracle<VoltDBGlobalState> {

    private static final String ERROR = "error: ";

    private final VoltDBGlobalState state;
    private String lastQuery;

    public VoltDBDurabilityOracle(VoltDBGlobalState state) {
        this.state = state;
    }

    @Override
    public void check() throws Exception {
        VoltDBConnection connection = (VoltDBConnection) state.getConnection();
        if (connection.getMirror() == null || connection.isDiverged()) {
            throw new IgnoreMeException();
        }
        List<String> differences = new ArrayList<>();
        for (VoltDBTable table : state.getSchema().getDatabaseTables()) {
            String sql = "SELECT COUNT(*) FROM " + table.getName();
            lastQuery = sql;
            String there = count(connection.getMirror(), sql);
            String here = count(connection, sql);
            // a change interrupted while the counts were read
            if (connection.isDiverged()) {
                throw new IgnoreMeException();
            }
            // a table neither cluster has (the provider's schema can be behind) is not a difference
            if (!here.equals(there) && !(here.startsWith(ERROR) && there.startsWith(ERROR))) {
                differences.add(table.getName() + ": " + here + " here, " + there + " on the mirror");
            }
        }
        if (!differences.isEmpty()) {
            throw new AssertionError("rows lost or gained across a crash (" + String.join("; ", differences) + ")");
        }
        // run-sqlancer.sh counts these: the databases shown to have kept every row across a crash
        if (connection.isFailureSeen()) {
            connection.clearFailureSeen();
            System.out.println("DURABLE across a crash: " + state.getDatabaseName() + ", "
                    + state.getSchema().getDatabaseTables().size() + " tables equal");
        }
    }

    // the count, or the error that stopped it ("object not found" on a table recovery lost)
    private static String count(VoltDBConnection connection, String sql) {
        try (Statement st = connection.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            rs.next();
            return Long.toString(rs.getLong(1));
        } catch (SQLException e) {
            return ERROR + e.getMessage();
        }
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
