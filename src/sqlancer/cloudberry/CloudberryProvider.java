package sqlancer.cloudberry;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.auto.service.AutoService;

import sqlancer.AbstractAction;
import sqlancer.DatabaseProvider;
import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.StatementExecutor;
import sqlancer.cloudberry.gen.CloudberryCommon;
import sqlancer.cloudberry.gen.CloudberryDiscardGenerator;
import sqlancer.cloudberry.gen.CloudberryTableGenerator;
import sqlancer.common.DBMSCommon;
import sqlancer.common.oracle.CompositeTestOracle;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.Query;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresOptions;
import sqlancer.postgres.PostgresProvider;
import sqlancer.postgres.PostgresSchema;
import sqlancer.postgres.PostgresSchema.PostgresDataType;
import sqlancer.postgres.PostgresSchema.PostgresTable;
import sqlancer.postgres.PostgresSchema.PostgresTable.TableType;

/**
 * SQLancer provider for Apache Cloudberry, a PostgreSQL-based MPP database. It extends the PostgreSQL provider and
 * reuses its generators and oracles. A single connection to the coordinator is enough. The Cloudberry-specific parts
 * are each table's distribution policy and leaf partitions for partitioned tables; storage formats (heap / ao_row /
 * ao_column / pax) come from the inherited {@code USING <am>} clause.
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

    // The inherited createDatabase() starts by dropping the database. A segment can still hold a
    // session on it for a moment ("database ... is being accessed by other users (seg0 ...)"), which
    // would kill the thread, so retry that case a few times.
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
        createTables(globalState, Randomly.fromOptions(4, 5, 6));
        for (PostgresTable table : globalState.getSchema().getDatabaseTables()) {
            if (table.isView() || table.getTableType() == TableType.TEMPORARY) {
                continue;
            }
            Character partitionStrategy = getPartitionStrategy(table.getName(), globalState);
            if (partitionStrategy != null) {
                // The inherited table generator creates partitioned tables without leaf partitions. Their
                // distribution policy follows the root, so they are not redistributed.
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
     * type-specific boundary literals. Some tables are deliberately left with zero leaf partitions: ORCA derives an
     * empty/Universal distribution for such a table, a shape worth keeping in the generated state space.
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
        Map<String, DistributionKeyHashing> eligibleColumns = getEligibleDistributionColumns(tableName, constraintTypes,
                globalState);

        String policyClause;
        String keyColumn = null;
        if (constraintTypes.isEmpty()) {
            // Unconstrained table: free choice among hash / random / replicated distribution.
            if (Randomly.getBooleanWithRatherLowProbability()) {
                policyClause = "REPLICATED";
            } else if (eligibleColumns.isEmpty() || Randomly.getBooleanWithSmallProbability()) {
                policyClause = "RANDOMLY";
            } else {
                keyColumn = Randomly.fromList(new ArrayList<>(eligibleColumns.keySet()));
                policyClause = "BY (" + keyColumn + ")";
            }
        } else {
            // With a PRIMARY KEY / UNIQUE constraint the distribution key must be a subset of it, and the table
            // cannot be distributed RANDOMLY or REPLICATED. If no single column satisfies all constraints, keep the
            // default (auto-assigned) distribution rather than issuing a doomed ALTER.
            if (eligibleColumns.isEmpty()) {
                return;
            }
            keyColumn = Randomly.fromList(new ArrayList<>(eligibleColumns.keySet()));
            policyClause = "BY (" + keyColumn + ")";
        }

        String queryString = "ALTER TABLE " + tableName + " SET DISTRIBUTED " + policyClause;
        SQLQueryAdapter alterTable = new SQLQueryAdapter(queryString, getCloudberryErrors());
        // With gp_use_legacy_hashops on, the key is hashed with the legacy cdbhash_*_ops operator class instead of
        // the type's default hash operator class. A type that has only a legacy class (money) can be a key only that
        // way; for any other type it is picked occasionally, so that tables hashed with legacy and default operator
        // classes coexist and the planner has to recognize that such tables are not co-located.
        boolean useLegacyHashops = keyColumn != null
                && (eligibleColumns.get(keyColumn) == DistributionKeyHashing.LEGACY_OPCLASS_ONLY
                        || Randomly.getBooleanWithRatherLowProbability());
        if (!useLegacyHashops) {
            globalState.executeStatement(alterTable);
            return;
        }
        globalState.executeStatement(new SQLQueryAdapter("SET gp_use_legacy_hashops = on"));
        try {
            globalState.executeStatement(alterTable);
        } finally {
            globalState.executeStatement(new SQLQueryAdapter("RESET gp_use_legacy_hashops"));
        }
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
     * Returns the column names eligible to be a single-column distribution key: columns that appear in every PRIMARY
     * KEY / UNIQUE / EXCLUDE constraint of the table (all columns, if the table is unconstrained).
     *
     * @param tableName
     *            the table to inspect
     * @param constraintTypes
     *            the constraint types already found on the table, as returned by {@link #getTableConstraints}
     * @param globalState
     *            the global state, used to obtain a connection
     *
     * @return the eligible column names, each mapped to how its type can be hashed as a distribution key
     */
    private static Map<String, DistributionKeyHashing> getEligibleDistributionColumns(String tableName,
            List<String> constraintTypes, PostgresGlobalState globalState) throws SQLException {
        Map<String, DistributionKeyHashing> eligible = new LinkedHashMap<>();
        if (constraintTypes.isEmpty()) {
            String queryString = "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '"
                    + tableName + "';";
            SQLQueryAdapter query = new SQLQueryAdapter(queryString);
            try (SQLancerResultSet rs = query.executeAndGet(globalState)) {
                if (rs == null) {
                    return eligible;
                }
                while (rs.next()) {
                    eligible.put(rs.getString(1), DistributionKeyHashing.fromDataType(rs.getString(2)));
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
        // A column is eligible only if it participates in all of the table's constraints.
        for (Map.Entry<String, Integer> e : columnConstraintCount.entrySet()) {
            if (e.getValue() == constraintTypes.size()) {
                eligible.put(e.getKey(), DistributionKeyHashing.fromDataType(columnType.get(e.getKey())));
            }
        }
        return eligible;
    }

    /**
     * How a column type can be hashed as a distribution key, which needs a hash operator class: the type's default one,
     * or, with {@code gp_use_legacy_hashops} on, a legacy {@code cdbhash_*_ops} one.
     */
    private enum DistributionKeyHashing {
        DEFAULT_OPCLASS, LEGACY_OPCLASS_ONLY;

        /**
         * Classifies a column type, failing on any type that has not been classified yet, so that a type newly added to
         * the generator cannot silently become an unusable distribution key.
         *
         * @param dataType
         *            the column's {@code information_schema} data type
         *
         * @return how the type can be hashed as a distribution key
         */
        static DistributionKeyHashing fromDataType(String dataType) {
            PostgresDataType type = PostgresSchema.getColumnType(dataType);
            switch (type) {
            case INT:
            case BOOLEAN:
            case TEXT:
            case DECIMAL:
            case FLOAT:
            case REAL:
            case RANGE:
            case BIT:
            case INET:
                return DEFAULT_OPCLASS;
            case MONEY:
                // money has no default hash operator class, only the legacy cdbhash_cash_ops.
                return LEGACY_OPCLASS_ONLY;
            default:
                throw new AssertionError(type);
            }
        }
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

    // Overridden solely to suppress one of the inherited actions (see mapCloudberryActions) and
    // to replace the generator of another (see CloudberryAction). Otherwise identical to
    // PostgresProvider.prepareTables(); every other action's generator and frequency come straight
    // from PostgresProvider.Action / PostgresProvider.mapActions.
    @Override
    protected void prepareTables(PostgresGlobalState globalState) throws Exception {
        CloudberryAction[] actions = Arrays.stream(PostgresProvider.Action.values()).map(CloudberryAction::new)
                .toArray(CloudberryAction[]::new);
        StatementExecutor<PostgresGlobalState, CloudberryAction> se = new StatementExecutor<>(globalState, actions,
                (state, a) -> mapCloudberryActions(state, a.action), (q) -> {
                    if (globalState.getSchema().getDatabaseTables().isEmpty()) {
                        throw new IgnoreMeException();
                    }
                });
        se.executeStatements();
        globalState.executeStatement(new SQLQueryAdapter("COMMIT", true));
        globalState.executeStatement(new SQLQueryAdapter("SET SESSION statement_timeout = 5000;\n"));
    }

    private static int mapCloudberryActions(PostgresGlobalState globalState, PostgresProvider.Action a) {
        // CREATE VIEW: the inherited generator often emits DISTINCT ON or LIMIT without a matching
        // ORDER BY. On an MPP cluster rows arrive from the segments in varying order, so such a view
        // can return different rows on each evaluation, which breaks oracles that compare two
        // evaluations (e.g. NoREC). Generating only materialized views would avoid this, but that
        // change belongs in the shared postgres generator.
        if (a == PostgresProvider.Action.CREATE_VIEW) {
            return 0;
        }
        return PostgresProvider.mapActions(globalState, a);
    }

    /**
     * A {@link PostgresProvider.Action} that runs the inherited generator, except for DISCARD, which runs
     * {@link CloudberryDiscardGenerator} so that no {@code DISCARD ALL} is issued.
     */
    private static final class CloudberryAction implements AbstractAction<PostgresGlobalState> {

        private final PostgresProvider.Action action;

        CloudberryAction(PostgresProvider.Action action) {
            this.action = action;
        }

        @Override
        public Query<?> getQuery(PostgresGlobalState globalState) throws Exception {
            if (action == PostgresProvider.Action.DISCARD) {
                return CloudberryDiscardGenerator.create(globalState);
            }
            return action.getQuery(globalState);
        }

        @Override
        public boolean canBeRetried() {
            return action.canBeRetried();
        }

    }

}
