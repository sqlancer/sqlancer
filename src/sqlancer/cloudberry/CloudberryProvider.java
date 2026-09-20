package sqlancer.cloudberry;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.auto.service.AutoService;

import sqlancer.DatabaseProvider;
import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.StatementExecutor;
import sqlancer.cloudberry.gen.CloudberryCommon;
import sqlancer.cloudberry.gen.CloudberryTableGenerator;
import sqlancer.common.DBMSCommon;
import sqlancer.common.oracle.CompositeTestOracle;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresOptions;
import sqlancer.postgres.PostgresProvider;
import sqlancer.postgres.PostgresSchema.PostgresTable;
import sqlancer.postgres.PostgresSchema.PostgresTable.TableType;

/**
 * SQLancer provider for Apache Cloudberry, a PostgreSQL-based MPP database. It extends the PostgreSQL provider and
 * reuses PostgreSQL's expression generators and NoREC/TLP oracles.
 *
 * <p>
 * Cloudberry is a transparent MPP system: a single connection to the coordinator is sufficient, so there is no
 * worker-node registration and no extension to create, and {@code createDatabase} does no more than wrap the inherited
 * one in a retry. The only Cloudberry-specific setup is assigning a distribution policy to each table via
 * {@code ALTER TABLE ... SET DISTRIBUTED}, choosing the distribution key so that it is a subset of any PRIMARY KEY /
 * UNIQUE constraint. Storage formats (heap / ao_row / ao_column / pax) are exercised for free through the inherited
 * {@code USING <am>} clause.
 */
@AutoService(DatabaseProvider.class)
public class CloudberryProvider extends PostgresProvider {

    private static final int CREATE_DATABASE_ATTEMPTS = 5;
    private static final int CREATE_DATABASE_RETRY_DELAY_MILLIS = 500;

    @SuppressWarnings("unchecked")
    public CloudberryProvider() {
        super((Class<PostgresGlobalState>) (Object) CloudberryGlobalState.class,
                (Class<PostgresOptions>) (Object) CloudberryOptions.class);
    }

    @Override
    public String getDBMSName() {
        return "cloudberry";
    }

    // PostgresProvider.createDatabase() opens each cycle by dropping the database it is about to
    // recreate, choosing "DROP DATABASE" or "DROP DATABASE FORCE" at random. Only the FORCE branch
    // has a fallback (it retries without FORCE); a plain DROP that loses the race against a session
    // still holding the database is rethrown, which kills the worker thread for the rest of the run.
    //
    // That race is specific to MPP: the failure observed here came back tagged with a segment
    // ("database ... is being accessed by other users (seg0 ...)"), meaning the coordinator's own
    // check had already passed and only a segment still held the database. It is intermittent --
    // seen once across many runs, and deliberate attempts to force it by holding client sessions on
    // the database did not reproduce it, since a client session is visible to the coordinator and so
    // fails the earlier check instead. The CI test runs with --num-queries 1000, which recreates the
    // database often, so a thread dying this way would surface as a flaky job rather than an honest
    // failure, which is what makes it worth defending against at all.
    //
    // Retrying the inherited method is preferable to reimplementing its ~80 lines of connection-URL
    // handling here: each attempt re-rolls that random FORCE choice, and gives a segment-local
    // leftover time to clear. Because the failure could not be reproduced on demand, the retry
    // branch itself is not covered by a test -- if it ever starts masking a real, persistent
    // failure, the attempt count is the thing to turn down.
    @Override
    public SQLConnection createDatabase(PostgresGlobalState globalState) throws SQLException {
        for (int attempt = 1; attempt < CREATE_DATABASE_ATTEMPTS; attempt++) {
            try {
                return super.createDatabase(globalState);
            } catch (SQLException e) {
                if (!isDatabaseStillInUse(e)) {
                    throw e;
                }
                waitForLingeringSession(e);
            }
        }
        // Final attempt: whatever it throws is a genuine failure, so let it propagate.
        return super.createDatabase(globalState);
    }

    private static boolean isDatabaseStillInUse(SQLException e) {
        String message = e.getMessage();
        return message != null && message.contains("is being accessed by other users");
    }

    private static void waitForLingeringSession(SQLException cause) throws SQLException {
        try {
            Thread.sleep(CREATE_DATABASE_RETRY_DELAY_MILLIS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw cause;
        }
    }

    @Override
    protected void createTables(PostgresGlobalState globalState, int numTables) throws Exception {
        while (globalState.getSchema().getDatabaseTables().size() < numTables) {
            try {
                String tableName = DBMSCommon.createTableName(globalState.getSchema().getDatabaseTables().size());
                SQLQueryAdapter createTable = CloudberryTableGenerator.generate(tableName, globalState.getSchema(),
                        generateOnlyKnown, globalState);
                globalState.executeStatement(createTable);
            } catch (IgnoreMeException e) {

            }
        }
    }

    @Override
    public void generateDatabase(PostgresGlobalState globalState) throws Exception {
        readFunctions(globalState);
        // The inherited PostgresTableGenerator already emits "PARTITION BY {RANGE,LIST,HASH}(...)" for
        // some tables (declarative partitioning), with no leaf partitions of its own. Give those tables
        // actual leaf partitions here rather than leaving every one permanently empty.
        createTables(globalState, Randomly.fromOptions(4, 5, 6));
        for (PostgresTable table : globalState.getSchema().getDatabaseTables()) {
            if (table.isView() || table.getTableType() == TableType.TEMPORARY) {
                continue;
            }
            Character partitionStrategy = getPartitionStrategy(table.getName(), globalState);
            if (partitionStrategy != null) {
                // Distribution policy of a partitioned table (and of its leaf partitions) cannot be set
                // independently -- it always follows the root -- so redistributeTable() does not apply here.
                createLeafPartitions(table.getName(), partitionStrategy, globalState);
                continue;
            }
            // Keep the default (auto-assigned) distribution for some tables to exercise that path too.
            if (Randomly.getBooleanWithRatherLowProbability()) {
                continue;
            }
            redistributeTable(table.getName(), globalState);
        }
        globalState.updateSchema();
        prepareTables(globalState);
    }

    /**
     * Returns the partition strategy of a declaratively partitioned table, or {@code null} if the table is not
     * partitioned.
     *
     * @param tableName
     *            the table to inspect
     * @param globalState
     *            the global state, used to obtain a connection
     *
     * @return {@code 'r'}=RANGE, {@code 'l'}=LIST, {@code 'h'}=HASH, or {@code null} if not partitioned
     */
    private static Character getPartitionStrategy(String tableName, PostgresGlobalState globalState)
            throws SQLException {
        String queryString = "SELECT pt.partstrat FROM pg_class c "
                + "JOIN pg_partitioned_table pt ON pt.partrelid = c.oid WHERE c.relname = '" + tableName + "';";
        SQLQueryAdapter query = new SQLQueryAdapter(queryString);
        try (SQLancerResultSet rs = query.executeAndGet(globalState)) {
            if (rs == null || !rs.next()) {
                return null;
            }
            String strat = rs.getString(1);
            return strat == null || strat.isEmpty() ? null : strat.charAt(0);
        }
    }

    /**
     * Adds leaf partitions to a declaratively partitioned table. HASH gets a small number of MODULUS/REMAINDER
     * partitions that together cover every row; RANGE and LIST get a single catch-all DEFAULT partition, which needs no
     * type-specific boundary literals. Some tables are deliberately left with zero leaf partitions: that exact shape (a
     * partitioned table ORCA derives an empty/Universal distribution for) is what originally triggered the "unexpected
     * gang size" crash in CPhysicalFullMergeJoin::PdsDerive, so it needs to stay in the generated state space.
     *
     * @param tableName
     *            the partitioned table to add leaf partitions to
     * @param strategy
     *            the table's partition strategy, as returned by {@link #getPartitionStrategy}
     * @param globalState
     *            the global state, used to execute the generated statements
     */
    private static void createLeafPartitions(String tableName, char strategy, PostgresGlobalState globalState)
            throws Exception {
        if (Randomly.getBooleanWithRatherLowProbability()) {
            return;
        }
        if (strategy == 'h') {
            int modulus = Randomly.fromOptions(2, 3, 4);
            for (int remainder = 0; remainder < modulus; remainder++) {
                String childName = tableName + "_h" + remainder;
                String queryString = "CREATE TABLE " + childName + " PARTITION OF " + tableName
                        + " FOR VALUES WITH (MODULUS " + modulus + ", REMAINDER " + remainder + ")";
                globalState.executeStatement(new SQLQueryAdapter(queryString, getCloudberryErrors()));
            }
        } else {
            String childName = tableName + "_default";
            String queryString = "CREATE TABLE " + childName + " PARTITION OF " + tableName + " DEFAULT";
            globalState.executeStatement(new SQLQueryAdapter(queryString, getCloudberryErrors()));
        }
    }

    /**
     * Re-assigns the distribution policy of a table. The distribution key must be a subset of every PRIMARY KEY /
     * UNIQUE constraint, so when constraints exist only their (intersected) columns are eligible. Falls back to
     * {@code RANDOMLY} when no eligible column exists, and occasionally chooses {@code REPLICATED} for unconstrained
     * tables.
     *
     * @param tableName
     *            the table to redistribute
     * @param globalState
     *            the global state, used to execute the generated statement
     */
    private static void redistributeTable(String tableName, PostgresGlobalState globalState) throws Exception {
        List<String> constraintTypes = getTableConstraints(tableName, globalState);
        List<String> eligibleColumns = getEligibleDistributionColumns(tableName, constraintTypes, globalState);

        String policyClause;
        if (constraintTypes.isEmpty()) {
            // Unconstrained table: free choice among hash / random / replicated distribution.
            if (Randomly.getBooleanWithRatherLowProbability()) {
                policyClause = "REPLICATED";
            } else if (eligibleColumns.isEmpty() || Randomly.getBooleanWithSmallProbability()) {
                policyClause = "RANDOMLY";
            } else {
                policyClause = "BY (" + Randomly.fromList(eligibleColumns) + ")";
            }
        } else {
            // With a PRIMARY KEY / UNIQUE constraint the distribution key must be a subset of it, and the table
            // cannot be distributed RANDOMLY or REPLICATED. If no single column satisfies all constraints, keep the
            // default (auto-assigned) distribution rather than issuing a doomed ALTER.
            if (eligibleColumns.isEmpty()) {
                return;
            }
            policyClause = "BY (" + Randomly.fromList(eligibleColumns) + ")";
        }

        String queryString = "ALTER TABLE " + tableName + " SET DISTRIBUTED " + policyClause;
        globalState.executeStatement(new SQLQueryAdapter(queryString, getCloudberryErrors()));
    }

    private static List<String> getTableConstraints(String tableName, PostgresGlobalState globalState)
            throws SQLException {
        List<String> constraints = new ArrayList<>();
        String queryString = "SELECT constraint_type FROM information_schema.table_constraints WHERE table_name = '"
                + tableName
                + "' AND (constraint_type = 'PRIMARY KEY' OR constraint_type = 'UNIQUE' OR constraint_type = 'EXCLUDE');";
        SQLQueryAdapter query = new SQLQueryAdapter(queryString);
        try (SQLancerResultSet rs = query.executeAndGet(globalState)) {
            if (rs == null) {
                return constraints;
            }
            while (rs.next()) {
                constraints.add(rs.getString(1));
            }
        }
        return constraints;
    }

    /**
     * Returns the column names eligible to be a single-column distribution key: hash-distributable columns that appear
     * in every PRIMARY KEY / UNIQUE / EXCLUDE constraint of the table (all columns, if the table is unconstrained).
     *
     * @param tableName
     *            the table to inspect
     * @param constraintTypes
     *            the constraint types already found on the table, as returned by {@link #getTableConstraints}
     * @param globalState
     *            the global state, used to obtain a connection
     *
     * @return the eligible column names
     */
    private static List<String> getEligibleDistributionColumns(String tableName, List<String> constraintTypes,
            PostgresGlobalState globalState) throws SQLException {
        List<String> eligible = new ArrayList<>();
        if (constraintTypes.isEmpty()) {
            String queryString = "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '"
                    + tableName + "';";
            SQLQueryAdapter query = new SQLQueryAdapter(queryString);
            try (SQLancerResultSet rs = query.executeAndGet(globalState)) {
                if (rs == null) {
                    return eligible;
                }
                while (rs.next()) {
                    String columnName = rs.getString(1);
                    String dataType = rs.getString(2);
                    if (isHashDistributable(dataType)) {
                        eligible.add(columnName);
                    }
                }
            }
            return eligible;
        }

        Map<String, Integer> columnConstraintCount = new HashMap<>();
        String queryString = "SELECT c.column_name, c.data_type FROM information_schema.table_constraints tc "
                + "JOIN information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name) "
                + "JOIN information_schema.columns AS c ON c.table_schema = tc.constraint_schema "
                + "AND tc.table_name = c.table_name AND ccu.column_name = c.column_name "
                + "WHERE (constraint_type = 'PRIMARY KEY' OR constraint_type = 'UNIQUE' OR constraint_type = 'EXCLUDE') "
                + "AND c.table_name = '" + tableName + "';";
        SQLQueryAdapter query = new SQLQueryAdapter(queryString);
        Map<String, String> columnType = new HashMap<>();
        try (SQLancerResultSet rs = query.executeAndGet(globalState)) {
            if (rs == null) {
                return eligible;
            }
            while (rs.next()) {
                String columnName = rs.getString(1);
                String dataType = rs.getString(2);
                columnType.put(columnName, dataType);
                columnConstraintCount.merge(columnName, 1, Integer::sum);
            }
        }
        // A column is eligible only if it participates in all of the table's constraints and is hash-distributable.
        for (Map.Entry<String, Integer> e : columnConstraintCount.entrySet()) {
            if (e.getValue() == constraintTypes.size() && isHashDistributable(columnType.get(e.getKey()))) {
                eligible.add(e.getKey());
            }
        }
        return eligible;
    }

    private static boolean isHashDistributable(String dataType) {
        // Types without a default hash operator class cannot be a Cloudberry distribution key.
        return !(dataType.equals("money") || dataType.equals("bit varying") || dataType.equals("xml")
                || dataType.equals("json") || dataType.startsWith("point") || dataType.startsWith("box")
                || dataType.startsWith("line") || dataType.startsWith("circle") || dataType.startsWith("polygon"));
    }

    @Override
    protected TestOracle<PostgresGlobalState> getTestOracle(PostgresGlobalState globalState) throws SQLException {
        List<TestOracle<PostgresGlobalState>> oracles = ((CloudberryOptions) globalState
                .getDbmsSpecificOptions()).cloudberryOracle.stream().map(o -> {
                    try {
                        return o.create(globalState);
                    } catch (Exception e) {
                        throw new AssertionError(e);
                    }
                }).collect(Collectors.toList());
        return new CompositeTestOracle<PostgresGlobalState>(oracles, globalState);
    }

    private static ExpectedErrors getCloudberryErrors() {
        ExpectedErrors errors = new ExpectedErrors();
        CloudberryCommon.addCloudberryErrors(errors);
        return errors;
    }

    // Overridden solely to suppress two of the inherited actions (see
    // mapActionsWithBugWorkarounds). Otherwise identical to PostgresProvider.prepareTables(); every
    // other action's generator and frequency come straight from PostgresProvider.Action /
    // PostgresProvider.mapActions.
    @Override
    protected void prepareTables(PostgresGlobalState globalState) throws Exception {
        StatementExecutor<PostgresGlobalState, PostgresProvider.Action> se = new StatementExecutor<>(globalState,
                PostgresProvider.Action.values(), CloudberryProvider::mapActionsWithBugWorkarounds, (q) -> {
                    if (globalState.getSchema().getDatabaseTables().isEmpty()) {
                        throw new IgnoreMeException();
                    }
                });
        se.executeStatements();
        globalState.executeStatement(new SQLQueryAdapter("COMMIT", true));
        globalState.executeStatement(new SQLQueryAdapter("SET SESSION statement_timeout = 5000;\n"));
    }

    private static int mapActionsWithBugWorkarounds(PostgresGlobalState globalState, PostgresProvider.Action a) {
        // CREATE STATISTICS: the "dependencies"-kind extended statistics this action generates can
        // crash the coordinator (see CloudberryBugs for the root cause). Unlike a normal expected
        // error, a crash disrupts unrelated concurrent sessions too, so whitelisting an error message
        // cannot handle it.
        if (CloudberryBugs.bugExtendedStatsDependenciesSegfault && a == PostgresProvider.Action.CREATE_STATISTICS) {
            return 0;
        }
        // CREATE VIEW: the inherited generator builds a view body with
        // PostgresRandomQueryGenerator, which readily emits DISTINCT ON or LIMIT with no matching
        // ORDER BY, and picks a non-materialized view about half the time. Such a view is re-evaluated
        // per reference and SQL leaves the surviving row unspecified, so on an MPP cluster -- where
        // the order rows arrive in from the segments varies between executions -- it can legitimately
        // return a different row each time. That breaks every oracle that compares two evaluations of
        // the same data: NoREC, for one, reported "the counts mismatch" on roughly half of its runs,
        // purely from a view defined as "SELECT DISTINCT ON ((0.5233916)::MONEY) c0 FROM ONLY t4".
        // Single-node PostgreSQL is much less exposed, since a small table tends to be scanned in the
        // same order every time, which is why the inherited generator does not guard against this.
        //
        // Views are worth testing, so this is meant to be temporary: restricting the generator to
        // MATERIALIZED views (whose contents are fixed when they are populated, and so are stable
        // across evaluations) would keep most of the coverage, but it belongs in the shared postgres
        // generator rather than here.
        if (a == PostgresProvider.Action.CREATE_VIEW) {
            return 0;
        }
        return PostgresProvider.mapActions(globalState, a);
    }

}
