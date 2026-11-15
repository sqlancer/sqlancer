package sqlancer.voltdb;

import java.nio.file.Paths;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import com.google.auto.service.AutoService;

import sqlancer.AbstractAction;
import sqlancer.DatabaseProvider;
import sqlancer.IgnoreMeException;
import sqlancer.MainOptions;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.SQLGlobalState;
import sqlancer.SQLProviderAdapter;
import sqlancer.StatementExecutor;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLQueryProvider;
import sqlancer.voltdb.gen.VoltDBDeleteGenerator;
import sqlancer.voltdb.gen.VoltDBIndexGenerator;
import sqlancer.voltdb.gen.VoltDBInsertGenerator;
import sqlancer.voltdb.gen.VoltDBTableGenerator;
import sqlancer.voltdb.gen.VoltDBUpdateGenerator;
import sqlancer.voltdb.gen.VoltDBViewGenerator;
import sqlancer.voltdb.oracle.VoltDBTransientCheck;

/**
 * SQLancer provider for VoltDB, connecting through the VoltDB JDBC driver.
 *
 * <p>
 * A VoltDB cluster has one schema, so each SQLancer database (one per thread) is the set of tables and indexes named
 * with its own prefix, for example {@code DATABASE0_T0}. Creating a database drops the tables left over from that
 * prefix's previous run.
 */
@AutoService(DatabaseProvider.class)
public class VoltDBProvider extends SQLProviderAdapter<VoltDBProvider.VoltDBGlobalState, VoltDBOptions> {

    // with --crashes, how long a connection is retried while the cluster recovers (run-sqlancer.sh gives a recovery
    // 600 s)
    private static final long CRASH_CONNECT_MILLIS = 660_000;

    private static final AtomicBoolean ROLES_CREATED = new AtomicBoolean();
    // how long a new database waits for XDCR to apply what is queued before it drops its name's leftovers (ENG-30630)
    private static final long DR_APPLIED_MILLIS = 60_000;

    public VoltDBProvider() {
        super(VoltDBGlobalState.class, VoltDBOptions.class);
    }

    public enum Action implements AbstractAction<VoltDBGlobalState> {
        INSERT(VoltDBInsertGenerator::getQuery), CREATE_INDEX(VoltDBIndexGenerator::getQuery),
        UPDATE(VoltDBUpdateGenerator::getQuery), DELETE(VoltDBDeleteGenerator::getQuery);

        private final SQLQueryProvider<VoltDBGlobalState> sqlQueryProvider;

        Action(SQLQueryProvider<VoltDBGlobalState> sqlQueryProvider) {
            this.sqlQueryProvider = sqlQueryProvider;
        }

        @Override
        public SQLQueryAdapter getQuery(VoltDBGlobalState state) throws Exception {
            return sqlQueryProvider.getQuery(state);
        }
    }

    private static int mapActions(VoltDBGlobalState globalState, Action a) {
        Randomly r = globalState.getRandomly();
        switch (a) {
        case INSERT:
            return r.getInteger(0, globalState.getOptions().getMaxNumberInserts());
        case CREATE_INDEX:
            return r.getInteger(0, globalState.getDbmsSpecificOptions().maxNumIndexes + 1);
        // fewer than the inserts, and few deletes, so that a database keeps rows for the oracles
        case UPDATE:
            return r.getInteger(0, globalState.getDbmsSpecificOptions().maxNumUpdates + 1);
        case DELETE:
            return r.getInteger(0, globalState.getDbmsSpecificOptions().maxNumDeletes + 1);
        default:
            throw new AssertionError(a);
        }
    }

    public static class VoltDBGlobalState extends SQLGlobalState<VoltDBOptions, VoltDBSchema> {

        private final AtomicInteger nextIndex = new AtomicInteger();
        private final AtomicInteger nextView = new AtomicInteger();
        // name to {base table, defining query}, in the order created; a view whose CREATE VIEW failed is in it too
        private final Map<String, String[]> views = Collections.synchronizedMap(new LinkedHashMap<>());

        // This database's name prefix, for example DATABASE0_.
        public String getPrefix() {
            return prefix(getDatabaseName());
        }

        public String getFreeTableName() {
            for (int i = 0;; i++) {
                String name = getPrefix() + "T" + i;
                if (getSchema().getDatabaseTables().stream().noneMatch(t -> t.getName().equals(name))) {
                    return name;
                }
            }
        }

        public String getFreeIndexName() {
            return getPrefix() + "I" + nextIndex.getAndIncrement();
        }

        public String getFreeViewName() {
            return getPrefix() + "V" + nextView.getAndIncrement();
        }

        public void addView(String name, String table, String query) {
            views.put(name, new String[] { table, query });
        }

        // name to {base table, defining query} of each view generated for this database
        public Map<String, String[]> getViews() {
            synchronized (views) {
                return new LinkedHashMap<>(views);
            }
        }

        @Override
        protected VoltDBSchema readSchema() throws SQLException {
            return VoltDBSchema.fromConnection(getConnection(), getPrefix());
        }
    }

    static String prefix(String databaseName) {
        return databaseName.toUpperCase(Locale.ROOT) + "_";
    }

    @Override
    public SQLConnection createDatabase(VoltDBGlobalState globalState) throws Exception {
        String host = globalState.getOptions().getHost();
        int port = globalState.getOptions().getPort();
        if (host == null) {
            host = VoltDBOptions.DEFAULT_HOST;
        }
        if (port == MainOptions.NO_SET_PORT) {
            port = VoltDBOptions.DEFAULT_PORT;
        }
        // the published voltdbclient jar does not register the driver through java.sql.Driver
        Class.forName("org.voltdb.jdbc.Driver");
        // With --servers the driver connects to every node and sends each call through any live connection, so a node
        // failure costs only the calls in flight on that node; topologychangeaware reconnects to a node once it has
        // rejoined
        String servers = globalState.getDbmsSpecificOptions().servers;
        String url = servers == null || servers.isBlank() ? String.format("jdbc:voltdb://%s:%d", host, port)
                : "jdbc:voltdb://" + servers.trim() + "?topologychangeaware=true";
        // a shrink (--resizes) removes hosts the driver is connected to (topologychangeaware), so the calls in flight
        // on them fail as on a killed node; the oracles' --node-failures workarounds (ENG-30612) do not apply
        // a crash (--crashes) fails every call in flight, as a killed node does for its own, and so does a node stopped
        // for an in-service upgrade (--upgrades) for the calls in flight on it
        // XDCR (milestone 23): the database writes through one of the pair, chosen at random, and repeats its DDL on
        // the other; each database's tables are written from one side only, so XDCR has no conflicts to resolve
        String xdcr = globalState.getDbmsSpecificOptions().xdcr;
        String peerUrl = null;
        if (xdcr != null && !xdcr.isBlank()) {
            peerUrl = "jdbc:voltdb://" + xdcr.trim() + (xdcr.contains(",") ? "?topologychangeaware=true" : "");
            if (Randomly.getBoolean()) {
                String here = url;
                url = peerUrl;
                peerUrl = here;
            }
        }
        VoltDBConnection sqlCon = connect(globalState, url,
                globalState.getDbmsSpecificOptions().nodeFailures || globalState.getDbmsSpecificOptions().resizes
                        || globalState.getDbmsSpecificOptions().crashes
                        || globalState.getDbmsSpecificOptions().upgrades);
        String hold = globalState.getDbmsSpecificOptions().ddlHold;
        if (hold != null && !hold.isBlank() && globalState.getDbmsSpecificOptions().avoidKnownBugs) {
            sqlCon.setDdlHold(Paths.get(hold));
        }
        if (peerUrl != null) {
            VoltDBConnection peerCon = connect(globalState, peerUrl, false);
            // ENG-30630: the earlier database of this name may have written through the other cluster, and rows still
            // on their way to this one when it drops and recreates the tables pause that DR stream for good; so
            // everything either cluster has queued for the other is applied first
            if (globalState.getDbmsSpecificOptions().avoidKnownBugs) {
                awaitDrApplied(sqlCon, peerCon);
            }
            dropLeftovers(globalState, sqlCon, true);
            dropLeftovers(globalState, peerCon, false);
            sqlCon.setPeer(peerCon);
        } else {
            dropLeftovers(globalState, sqlCon, true);
        }
        // DIFF (milestone 20): a second cluster, normally the last release, that every change is repeated on
        String mirror = globalState.getDbmsSpecificOptions().mirror;
        if (mirror != null && !mirror.isBlank()) {
            VoltDBConnection mirrorCon = connect(globalState,
                    "jdbc:voltdb://" + mirror.trim() + (mirror.contains(",") ? "?topologychangeaware=true" : ""),
                    false);
            dropLeftovers(globalState, mirrorCon, false);
            sqlCon.setMirror(mirrorCon);
        }
        // the PERMS oracle's roles, once per run (a statement through sqlCon also runs on the mirror)
        if (globalState.getDbmsSpecificOptions().security && ROLES_CREATED.compareAndSet(false, true)) {
            for (String role : new String[] { "CREATE ROLE sqlancer_read WITH SQLREAD",
                    "CREATE ROLE sqlancer_write WITH SQL" }) {
                try (Statement st = sqlCon.createStatement()) {
                    st.execute(role);
                } catch (SQLException e) {
                    if (!e.getMessage().contains("already exists")) {
                        throw e;
                    }
                }
            }
        }
        return sqlCon;
    }

    private static VoltDBConnection connect(VoltDBGlobalState globalState, String url, boolean nodeFailures)
            throws SQLException {
        long deadline = System.currentTimeMillis() + CRASH_CONNECT_MILLIS;
        while (true) {
            try {
                Connection con = DriverManager.getConnection(url, globalState.getOptions().getUserName(),
                        globalState.getOptions().getPassword());
                return new VoltDBConnection(con, globalState.getDbmsSpecificOptions().retrySeconds, nodeFailures);
            } catch (SQLException e) {
                if (!globalState.getDbmsSpecificOptions().crashes || System.currentTimeMillis() >= deadline) {
                    throw e;
                }
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw e;
                }
            }
        }
    }

    // waits, for up to DR_APPLIED_MILLIS, until each cluster's DR producer has had acknowledged every transaction it
    // had
    // queued for the other when this was called (other databases keep writing, so the queues are seldom empty)
    private static void awaitDrApplied(VoltDBConnection... clusters) throws SQLException {
        List<Map<String, long[]>> queued = new ArrayList<>();
        for (VoltDBConnection c : clusters) {
            queued.add(drProducer(c));
        }
        long deadline = System.currentTimeMillis() + DR_APPLIED_MILLIS;
        for (int i = 0; i < clusters.length; i++) {
            while (true) {
                Map<String, long[]> now = drProducer(clusters[i]);
                boolean applied = true;
                for (Map.Entry<String, long[]> e : queued.get(i).entrySet()) {
                    long[] n = now.get(e.getKey());
                    if (n != null && n[1] < e.getValue()[0]) {
                        applied = false;
                    }
                }
                if (applied || System.currentTimeMillis() >= deadline) {
                    break;
                }
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    // each DR producer partition's last queued and last acknowledged DR id, by host, partition and remote cluster
    private static Map<String, long[]> drProducer(VoltDBConnection c) throws SQLException {
        Map<String, long[]> ids = new LinkedHashMap<>();
        try (CallableStatement call = c.prepareCall("{call @Statistics(?, ?)}")) {
            call.setString(1, "DRPRODUCERPARTITION");
            call.setInt(2, 0);
            try (ResultSet rs = call.executeQuery()) {
                while (rs.next()) {
                    ids.put(rs.getLong("HOST_ID") + "/" + rs.getLong("PARTITION_ID") + "/"
                            + rs.getLong("REMOTE_CLUSTER_ID"),
                            new long[] { rs.getLong("LASTQUEUEDDRID"), rs.getLong("LASTACKDRID") });
                }
            }
        }
        return ids;
    }

    // drops the database's procedures and tables left by an earlier use of its name; logged only for the main cluster,
    // whose statements a reproduction replays
    private static void dropLeftovers(VoltDBGlobalState globalState, VoltDBConnection sqlCon, boolean log)
            throws SQLException {
        DatabaseMetaData md = sqlCon.getMetaData();
        // a procedure the PROCEDURE oracle created and a node failure kept it from dropping; it reads the tables below
        List<String> drops = new ArrayList<>();
        try (ResultSet rs = md.getProcedures(null, null, null)) {
            while (rs.next()) {
                String name = rs.getString("PROCEDURE_NAME");
                // the PROCEDURE oracle's names are the prefix, P and a number; the tables' default procedures, such as
                // DB0_T0.insert, go with their tables
                if (name.matches(Pattern.quote(prefix(globalState.getDatabaseName())) + "P[0-9]+")) {
                    drops.add("DROP PROCEDURE " + name + " IF EXISTS");
                }
            }
        }
        for (String[] t : VoltDBSchema.getTables(md, prefix(globalState.getDatabaseName()))) {
            drops.add("DROP TABLE " + t[0] + " IF EXISTS CASCADE");
        }
        for (String drop : drops) {
            if (log) {
                globalState.getState().logStatement(drop);
            }
            try (Statement s = sqlCon.createStatement()) {
                s.execute(drop);
            }
        }
    }

    // ENG-30602: unless --avoid-known-bugs is false, a failure that does not recur when its queries are rerun at once
    // is counted, not reported (see VoltDBTransientCheck)
    @Override
    protected TestOracle<VoltDBGlobalState> getTestOracle(VoltDBGlobalState globalState) throws Exception {
        TestOracle<VoltDBGlobalState> oracle = super.getTestOracle(globalState);
        return globalState.getDbmsSpecificOptions().avoidKnownBugs ? new VoltDBTransientCheck(oracle, globalState)
                : oracle;
    }

    @Override
    public void generateDatabase(VoltDBGlobalState globalState) throws Exception {
        int nrTables = 1 + (int) Randomly.getNotCachedInteger(0, globalState.getDbmsSpecificOptions().maxNumTables);
        for (int i = 0; i < nrTables; i++) {
            boolean success;
            do {
                success = globalState.executeStatement(VoltDBTableGenerator.getQuery(globalState));
            } while (!success);
        }
        if (globalState.getSchema().getDatabaseTables().isEmpty()) {
            throw new IgnoreMeException();
        }
        // Views come before any rows: VoltDB cannot create some views on a table that holds data ("cannot always be
        // applied if the table already contains data"), and a view made first is kept up to date through every insert,
        // update and delete that follows.
        int nrViews = globalState.getRandomly().getInteger(0, globalState.getDbmsSpecificOptions().maxNumViews + 1);
        for (int i = 0; i < nrViews; i++) {
            try {
                globalState.executeStatement(VoltDBViewGenerator.getQuery(globalState));
            } catch (IgnoreMeException e) {
                // no view this time
            }
        }
        // with --xdcr, every table is a DR table, on both clusters, before it holds any rows
        String xdcr = globalState.getDbmsSpecificOptions().xdcr;
        if (xdcr != null && !xdcr.isBlank()) {
            for (VoltDBSchema.VoltDBTable table : globalState.getSchema().getDatabaseTables()) {
                globalState.executeStatement(
                        new SQLQueryAdapter("DR TABLE " + table.getName(), ExpectedErrors.newErrors().build(), true));
            }
        }
        StatementExecutor<VoltDBGlobalState, Action> se = new StatementExecutor<>(globalState, Action.values(),
                VoltDBProvider::mapActions, q -> {
                    if (globalState.getSchema().getDatabaseTables().isEmpty()) {
                        throw new IgnoreMeException();
                    }
                });
        se.executeStatements();
    }

    @Override
    public String getDBMSName() {
        return "voltdb";
    }

}
