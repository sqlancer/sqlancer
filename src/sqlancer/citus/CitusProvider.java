package sqlancer.citus;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.AbstractAction;
import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.StatementExecutor;
import sqlancer.citus.gen.CitusAlterTableGenerator;
import sqlancer.citus.gen.CitusCommon;
import sqlancer.citus.gen.CitusDeleteGenerator;
import sqlancer.citus.gen.CitusIndexGenerator;
import sqlancer.citus.gen.CitusInsertGenerator;
import sqlancer.citus.gen.CitusSetGenerator;
import sqlancer.citus.gen.CitusUpdateGenerator;
import sqlancer.citus.gen.CitusViewGenerator;
import sqlancer.common.oracle.CompositeTestOracle;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.Query;
import sqlancer.common.query.QueryAdapter;
import sqlancer.common.query.QueryProvider;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresOptions;
import sqlancer.postgres.PostgresProvider;
import sqlancer.postgres.PostgresSchema;
import sqlancer.postgres.PostgresSchema.PostgresColumn;
import sqlancer.postgres.PostgresSchema.PostgresTable;
import sqlancer.postgres.PostgresSchema.PostgresTable.TableType;
import sqlancer.postgres.gen.PostgresAnalyzeGenerator;
import sqlancer.postgres.gen.PostgresClusterGenerator;
import sqlancer.postgres.gen.PostgresCommentGenerator;
import sqlancer.postgres.gen.PostgresDiscardGenerator;
import sqlancer.postgres.gen.PostgresDropIndexGenerator;
import sqlancer.postgres.gen.PostgresNotifyGenerator;
import sqlancer.postgres.gen.PostgresQueryCatalogGenerator;
import sqlancer.postgres.gen.PostgresReindexGenerator;
import sqlancer.postgres.gen.PostgresSequenceGenerator;
import sqlancer.postgres.gen.PostgresStatisticsGenerator;
import sqlancer.postgres.gen.PostgresTransactionGenerator;
import sqlancer.postgres.gen.PostgresTruncateGenerator;
import sqlancer.postgres.gen.PostgresVacuumGenerator;

public class CitusProvider extends PostgresProvider {

    @SuppressWarnings("unchecked")
    public CitusProvider() {
        super((Class<PostgresGlobalState>) (Object) CitusGlobalState.class,
                (Class<PostgresOptions>) (Object) CitusOptions.class);
    }

    public enum Action implements AbstractAction<PostgresGlobalState> {
        ANALYZE(PostgresAnalyzeGenerator::create), //
        ALTER_TABLE(g -> CitusAlterTableGenerator.create(g.getSchema().getRandomTable(t -> !t.isView()), g,
                generateOnlyKnown)), //
        CLUSTER(PostgresClusterGenerator::create), //
        COMMIT(g -> {
            Query query;
            if (Randomly.getBoolean()) {
                query = new QueryAdapter("COMMIT", true);
            } else if (Randomly.getBoolean()) {
                query = PostgresTransactionGenerator.executeBegin();
            } else {
                query = new QueryAdapter("ROLLBACK", true);
            }
            return query;
        }), //
        CREATE_STATISTICS(PostgresStatisticsGenerator::insert), //
        DROP_STATISTICS(PostgresStatisticsGenerator::remove), //
        DELETE(CitusDeleteGenerator::create), //
        DISCARD(PostgresDiscardGenerator::create), //
        DROP_INDEX(PostgresDropIndexGenerator::create), //
        INSERT(CitusInsertGenerator::insert), //
        UPDATE(CitusUpdateGenerator::create), //
        TRUNCATE(PostgresTruncateGenerator::create), //
        VACUUM(PostgresVacuumGenerator::create), //
        REINDEX(PostgresReindexGenerator::create), //
        SET(CitusSetGenerator::create), //
        CREATE_INDEX(CitusIndexGenerator::generate), //
        SET_CONSTRAINTS((g) -> {
            StringBuilder sb = new StringBuilder();
            sb.append("SET CONSTRAINTS ALL ");
            sb.append(Randomly.fromOptions("DEFERRED", "IMMEDIATE"));
            return new QueryAdapter(sb.toString());
        }), //
        RESET_ROLE((g) -> new QueryAdapter("RESET ROLE")), //
        COMMENT_ON(PostgresCommentGenerator::generate), //
        RESET((g) -> new QueryAdapter("RESET ALL") /*
                                                    * https://www.postgresql.org/docs/devel/sql-reset.html TODO: also
                                                    * configuration parameter
                                                    */), //
        NOTIFY(PostgresNotifyGenerator::createNotify), //
        LISTEN((g) -> PostgresNotifyGenerator.createListen()), //
        UNLISTEN((g) -> PostgresNotifyGenerator.createUnlisten()), //
        CREATE_SEQUENCE(PostgresSequenceGenerator::createSequence), //
        CREATE_VIEW(CitusViewGenerator::create), //
        QUERY_CATALOG((g) -> PostgresQueryCatalogGenerator.query());

        private final QueryProvider<PostgresGlobalState> queryProvider;

        Action(QueryProvider<PostgresGlobalState> queryProvider) {
            this.queryProvider = queryProvider;
        }

        @Override
        public Query getQuery(PostgresGlobalState state) throws Exception {
            return queryProvider.getQuery(state);
        }
    }

    private static int mapActions(PostgresGlobalState globalState, Action a) {
        Randomly r = globalState.getRandomly();
        int nrPerformed;
        switch (a) {
        case CREATE_INDEX:
        case CLUSTER:
            nrPerformed = r.getInteger(0, 3);
            break;
        case CREATE_STATISTICS:
            nrPerformed = r.getInteger(0, 5);
            break;
        case DISCARD:
        case DROP_INDEX:
            nrPerformed = r.getInteger(0, 5);
            break;
        case COMMIT:
            nrPerformed = r.getInteger(0, 0);
            break;
        case ALTER_TABLE:
            nrPerformed = r.getInteger(0, 5);
            break;
        case REINDEX:
        case RESET:
            nrPerformed = r.getInteger(0, 3);
            break;
        case DELETE:
        case RESET_ROLE:
        case SET:
        case QUERY_CATALOG:
            nrPerformed = r.getInteger(0, 5);
            break;
        case ANALYZE:
            nrPerformed = r.getInteger(0, 3);
            break;
        case VACUUM:
        case SET_CONSTRAINTS:
        case COMMENT_ON:
        case NOTIFY:
        case LISTEN:
        case UNLISTEN:
        case CREATE_SEQUENCE:
        case DROP_STATISTICS:
        case TRUNCATE:
            nrPerformed = r.getInteger(0, 2);
            break;
        case CREATE_VIEW:
            nrPerformed = r.getInteger(0, 2);
            break;
        case UPDATE:
            nrPerformed = r.getInteger(0, 10);
            break;
        case INSERT:
            nrPerformed = r.getInteger(0, globalState.getOptions().getMaxNumberInserts());
            break;
        default:
            throw new AssertionError(a);
        }
        return nrPerformed;

    }

    private class CitusWorkerNode {

        private final String host;
        private final int port;

        CitusWorkerNode(String nodeHost, int nodePort) {
            this.host = nodeHost;
            this.port = nodePort;
        }

        public String getHost() {
            return this.host;
        }

        public int getPort() {
            return this.port;
        }

    }

    private static void distributeTable(List<PostgresColumn> columns, String tableName, CitusGlobalState globalState)
            throws Exception {
        if (!columns.isEmpty()) {
            PostgresColumn columnToDistribute = Randomly.fromList(columns);
            String queryString = "SELECT create_distributed_table('" + tableName + "', '" + columnToDistribute.getName()
                    + "');";
            QueryAdapter query = new QueryAdapter(queryString, getCitusErrors());
            globalState.executeStatement(query, "SELECT create_distributed_table(?, ?);", tableName,
                    columnToDistribute.getName());
        }
    }

    private static List<String> getTableConstraints(String tableName, CitusGlobalState globalState)
            throws SQLException {
        List<String> constraints = new ArrayList<>();
        String queryString = "SELECT constraint_type FROM information_schema.table_constraints WHERE table_name = '"
                + tableName
                + "' AND (constraint_type = 'PRIMARY KEY' OR constraint_type = 'UNIQUE' or constraint_type = 'EXCLUDE');";
        QueryAdapter query = new QueryAdapter(queryString);
        SQLancerResultSet rs = query.executeAndGet(globalState,
                "SELECT constraint_type FROM information_schema.table_constraints WHERE table_name = ? AND (constraint_type = 'PRIMARY KEY' OR constraint_type = 'UNIQUE' or constraint_type = 'EXCLUDE');",
                tableName);
        while (rs.next()) {
            constraints.add(rs.getString(1));
        }
        return constraints;
    }

    private static void createDistributedTable(String tableName, CitusGlobalState globalState) throws Exception {
        List<PostgresColumn> columns = new ArrayList<>();
        List<String> tableConstraints = getTableConstraints(tableName, globalState);
        if (tableConstraints.isEmpty()) {
            String queryString = "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '"
                    + tableName + "';";
            QueryAdapter query = new QueryAdapter(queryString);
            SQLancerResultSet rs = query.executeAndGet(globalState,
                    "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = ?;", tableName);
            while (rs.next()) {
                String columnName = rs.getString(1);
                String dataType = rs.getString(2);
                if (dataTypeHasDefaultOperatorForPartition(dataType)) {
                    PostgresColumn c = new PostgresColumn(columnName, PostgresSchema.getColumnType(dataType));
                    columns.add(c);
                }
            }
        } else {
            HashMap<PostgresColumn, List<String>> columnConstraints = new HashMap<>();
            String queryString = "SELECT c.column_name, c.data_type, tc.constraint_type FROM information_schema.table_constraints tc JOIN information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name) JOIN information_schema.columns AS c ON c.table_schema = tc.constraint_schema AND tc.table_name = c.table_name AND ccu.column_name = c.column_name WHERE (constraint_type = 'PRIMARY KEY' OR constraint_type = 'UNIQUE' OR constraint_type = 'EXCLUDE') AND c.table_name = '"
                    + tableName + "';";
            QueryAdapter query = new QueryAdapter(queryString);
            SQLancerResultSet rs = query.executeAndGet(globalState,
                    "SELECT c.column_name, c.data_type, tc.constraint_type FROM information_schema.table_constraints tc JOIN information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name) JOIN information_schema.columns AS c ON c.table_schema = tc.constraint_schema AND tc.table_name = c.table_name AND ccu.column_name = c.column_name WHERE (constraint_type = 'PRIMARY KEY' OR constraint_type = 'UNIQUE' OR constraint_type = 'EXCLUDE') AND c.table_name = ?;",
                    tableName);
            while (rs.next()) {
                String columnName = rs.getString(1);
                String dataType = rs.getString(2);
                String constraintType = rs.getString(3);
                if (dataTypeHasDefaultOperatorForPartition(dataType)) {
                    PostgresColumn c = new PostgresColumn(columnName, PostgresSchema.getColumnType(dataType));
                    if (columnConstraints.containsKey(c)) {
                        columnConstraints.get(c).add(constraintType);
                    } else {
                        columnConstraints.put(c, new ArrayList<>(Arrays.asList(constraintType)));
                    }
                }
            }
            for (PostgresColumn c : columnConstraints.keySet()) {
                // check if all table contraints are included in column constraints, i.e. column eligible to distribute
                if (tableConstraints.size() == columnConstraints.get(c).size()) {
                    columns.add(c);
                }
            }
        }
        distributeTable(columns, tableName, globalState);
    }

    @Override
    public void generateDatabase(PostgresGlobalState globalState) throws Exception {
        readFunctions(globalState);
        createTables(globalState, Randomly.fromOptions(4, 5, 6));
        for (PostgresTable table : globalState.getSchema().getDatabaseTables()) {
            if (!(table.getTableType() == TableType.TEMPORARY || Randomly.getBooleanWithRatherLowProbability())) {
                if (Randomly.getBooleanWithRatherLowProbability()) {
                    // create reference table
                    String queryString = "SELECT create_reference_table('" + table.getName() + "');";
                    QueryAdapter query = new QueryAdapter(queryString, getCitusErrors());
                    globalState.executeStatement(query, "SELECT create_reference_table(?);", table.getName());
                } else {
                    // create distributed table
                    createDistributedTable(table.getName(), (CitusGlobalState) globalState);
                }
            }
            // else: keep local table
        }
        globalState.updateSchema();
        prepareTables(globalState);
        if (((CitusGlobalState) globalState).getRepartition()) {
            // allow repartition joins
            globalState.executeStatement(
                    new QueryAdapter("SET citus.enable_repartition_joins to ON;\n", getCitusErrors()));
        }
    }

    @Override
    protected TestOracle getTestOracle(PostgresGlobalState globalState) throws SQLException {
        List<TestOracle> oracles = ((CitusOptions) globalState.getDmbsSpecificOptions()).citusOracle.stream().map(o -> {
            try {
                return o.create(globalState);
            } catch (Exception e1) {
                throw new AssertionError(e1);
            }
        }).collect(Collectors.toList());
        return new CompositeTestOracle(oracles, globalState);
    }

    private List<CitusWorkerNode> readCitusWorkerNodes(PostgresGlobalState globalState, Connection con)
            throws SQLException {
        globalState.getState().logStatement("SELECT * FROM master_get_active_worker_nodes()");
        List<CitusWorkerNode> citusWorkerNodes = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT * FROM master_get_active_worker_nodes();");
            while (rs.next()) {
                String nodeHost = rs.getString("node_name");
                int nodePort = rs.getInt("node_port");
                CitusWorkerNode w = new CitusWorkerNode(nodeHost, nodePort);
                citusWorkerNodes.add(w);
            }
        }
        return citusWorkerNodes;
    }

    private void addCitusExtension(PostgresGlobalState globalState, Connection con) throws SQLException {
        globalState.getState().logStatement("CREATE EXTENSION citus;");
        try (Statement s = con.createStatement()) {
            s.execute("CREATE EXTENSION citus;");
        }
    }

    private void prepareCitusWorkerNodes(PostgresGlobalState globalState, List<CitusWorkerNode> citusWorkerNodes,
            int databaseIndex, String entryDatabaseName) throws SQLException {
        for (CitusWorkerNode w : citusWorkerNodes) {
            // connect to worker node, entry database
            int hostIndex = entryURL.indexOf(host);
            String preHost = entryURL.substring(0, hostIndex);
            String postHost = entryURL.substring(databaseIndex - 1);
            String entryWorkerURL = preHost + w.getHost() + ":" + w.getPort() + postHost;
            globalState.getState().logStatement("\\q");
            globalState.getState().logStatement(entryWorkerURL);
            Connection con = DriverManager.getConnection("jdbc:" + entryWorkerURL, username, password);

            // create test database at worker node
            globalState.getState().logStatement("DROP DATABASE IF EXISTS " + databaseName);
            globalState.getState().logStatement(createDatabaseCommand);
            try (Statement s = con.createStatement()) {
                s.execute("DROP DATABASE IF EXISTS " + databaseName);
            }
            try (Statement s = con.createStatement()) {
                s.execute(createDatabaseCommand);
            }
            con.close();

            // connect to worker node, test database
            int databaseIndexWorker = entryWorkerURL.indexOf(entryPath) + 1;
            String preDatabaseNameWorker = entryWorkerURL.substring(0, databaseIndexWorker);
            String postDatabaseNameWorker = entryWorkerURL.substring(databaseIndexWorker + entryDatabaseName.length());
            String testWorkerURL = preDatabaseNameWorker + databaseName + postDatabaseNameWorker;
            globalState.getState().logStatement(String.format("\\c %s;", databaseName));
            con = DriverManager.getConnection("jdbc:" + testWorkerURL, username, password);

            // add citus extension to worker node, test database
            addCitusExtension(globalState, con);
            con.close();
        }
    }

    private void addCitusWorkerNodes(PostgresGlobalState globalState, Connection con,
            List<CitusWorkerNode> citusWorkerNodes) throws SQLException {
        for (CitusWorkerNode w : citusWorkerNodes) {
            String addWorkers = "SELECT * from master_add_node('" + w.getHost() + "', " + w.getPort() + ");";
            globalState.getState().logStatement(addWorkers);
            try (Statement s = con.createStatement()) {
                s.execute(addWorkers);
            }
        }
    }

    @Override
    public Connection createDatabase(PostgresGlobalState globalState) throws SQLException {
        synchronized (CitusProvider.class) {
            // returns connection to coordinator node, test database
            Connection con = super.createDatabase(globalState);
            String entryDatabaseName = entryPath.substring(1);
            int databaseIndex = entryURL.indexOf(entryPath) + 1;
            // add citus extension to coordinator node, test database
            addCitusExtension(globalState, con);
            con.close();

            // reconnect to coordinator node, entry database
            globalState.getState().logStatement(String.format("\\c %s;", entryDatabaseName));
            con = DriverManager.getConnection("jdbc:" + entryURL, username, password);
            // read info about worker nodes
            List<CitusWorkerNode> citusWorkerNodes = readCitusWorkerNodes(globalState, con);
            con.close();

            // prepare worker nodes for test database
            prepareCitusWorkerNodes(globalState, citusWorkerNodes, databaseIndex, entryDatabaseName);

            // reconnect to coordinator node, test database
            globalState.getState().logStatement("\\q");
            globalState.getState().logStatement(testURL);
            con = DriverManager.getConnection("jdbc:" + testURL, username, password);
            // add worker nodes to coordinator node for test database
            addCitusWorkerNodes(globalState, con, citusWorkerNodes);
            con.close();

            // reconnect to coordinator node, test database
            con = DriverManager.getConnection("jdbc:" + testURL, username, password);
            ((CitusGlobalState) globalState)
                    .setRepartition(((CitusOptions) globalState.getDmbsSpecificOptions()).repartition);
            globalState.getState().commentStatements();
            return con;
        }
    }

    @Override
    protected void prepareTables(PostgresGlobalState globalState) throws Exception {
        StatementExecutor<PostgresGlobalState, Action> se = new StatementExecutor<>(globalState, Action.values(),
                CitusProvider::mapActions, (q) -> {
                    if (globalState.getSchema().getDatabaseTables().isEmpty()) {
                        throw new IgnoreMeException();
                    }
                });
        se.executeStatements();
        globalState.executeStatement(new QueryAdapter("COMMIT", true));
        globalState.executeStatement(new QueryAdapter("SET SESSION statement_timeout = 5000;\n"));
    }

    @Override
    public String getDBMSName() {
        return "citus";
    }

    private static ExpectedErrors getCitusErrors() {
        ExpectedErrors errors = new ExpectedErrors();
        CitusCommon.addCitusErrors(errors);
        return errors;
    }

    private static boolean dataTypeHasDefaultOperatorForPartition(String dataType) {
        return !(dataType.equals("money") || dataType.equals("bit varying"));
    }

}
