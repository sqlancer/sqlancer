package sqlancer.citus;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import sqlancer.AbstractAction;
import sqlancer.IgnoreMeException;
import sqlancer.Query;
import sqlancer.QueryAdapter;
import sqlancer.QueryProvider;
import sqlancer.Randomly;
import sqlancer.StatementExecutor;
import sqlancer.citus.gen.CitusAlterTableGenerator;
import sqlancer.citus.gen.CitusCommon;
import sqlancer.citus.gen.CitusDeleteGenerator;
import sqlancer.citus.gen.CitusInsertGenerator;
import sqlancer.citus.gen.CitusSetGenerator;
import sqlancer.citus.gen.CitusTableGenerator;
import sqlancer.citus.gen.CitusUpdateGenerator;
import sqlancer.citus.gen.CitusViewGenerator;
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
import sqlancer.postgres.gen.PostgresIndexGenerator;
import sqlancer.postgres.gen.PostgresNotifyGenerator;
import sqlancer.postgres.gen.PostgresQueryCatalogGenerator;
import sqlancer.postgres.gen.PostgresReindexGenerator;
import sqlancer.postgres.gen.PostgresSequenceGenerator;
import sqlancer.postgres.gen.PostgresStatisticsGenerator;
import sqlancer.postgres.gen.PostgresTransactionGenerator;
import sqlancer.postgres.gen.PostgresTruncateGenerator;
import sqlancer.postgres.gen.PostgresVacuumGenerator;
import sqlancer.sqlite3.gen.SQLite3Common;

public class CitusProvider extends PostgresProvider {

    private static Set<String> errors = new HashSet<>();

    @SuppressWarnings("unchecked")
    public CitusProvider() {
        super((Class<PostgresGlobalState>) (Object) CitusGlobalState.class,
                (Class<PostgresOptions>) (Object) CitusOptions.class);
        CitusCommon.addCitusErrors(errors);
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
        CREATE_INDEX(PostgresIndexGenerator::generate), //
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
        public Query getQuery(PostgresGlobalState state) throws SQLException {
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

    private class WorkerNode {

        private final String host;
        private final int port;

        WorkerNode(String nodeHost, int nodePort) {
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

    private static void distributeTable(List<PostgresColumn> columns, String tableName, CitusGlobalState globalState,
            Connection con) throws SQLException {
        if (columns.size() != 0) {
            PostgresColumn columnToDistribute = Randomly.fromList(columns);
            String template = "SELECT create_distributed_table(?, ?);";
            QueryAdapter query = new QueryAdapter(template, errors);
            globalState.executeStatement(query, template, tableName, columnToDistribute.getName());
            // distribution column cannot take NULL value
            // TODO: find a way to protect from SQL injection without '' around string input
            query = new QueryAdapter(
                    "ALTER TABLE " + tableName + " ALTER COLUMN " + columnToDistribute.getName() + " SET NOT NULL;",
                    errors);
            globalState.executeStatement(query);
        }
    }

    private static List<String> getTableConstraints(String tableName, CitusGlobalState globalState, Connection con)
            throws SQLException {
        List<String> constraints = new ArrayList<>();
        String template = "SELECT constraint_type FROM information_schema.table_constraints WHERE table_name = ? AND (constraint_type = 'PRIMARY KEY' OR constraint_type = 'UNIQUE' or constraint_type = 'EXCLUDE');";
        QueryAdapter query = new QueryAdapter(template);
        ResultSet rs = query.executeAndGet(globalState, template, tableName);
        while (rs.next()) {
            constraints.add(rs.getString("constraint_type"));
        }
        return constraints;
    }

    private static void createDistributedTable(String tableName, CitusGlobalState globalState, Connection con)
            throws SQLException {
        List<PostgresColumn> columns = new ArrayList<>();
        List<String> tableConstraints = getTableConstraints(tableName, globalState, con);
        if (tableConstraints.size() == 0) {
            String template = "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = ?;";
            QueryAdapter query = new QueryAdapter(template);
            ResultSet rs = query.executeAndGet(globalState, template, tableName);
            while (rs.next()) {
                String columnName = rs.getString("column_name");
                String dataType = rs.getString("data_type");
                // data types money & bit varying have no default operator class for specified partition method
                if (!(dataType.equals("money") || dataType.equals("bit varying"))) {
                    PostgresColumn c = new PostgresColumn(columnName, PostgresSchema.getColumnType(dataType));
                    columns.add(c);
                }
            }
        } else {
            HashMap<PostgresColumn, List<String>> columnConstraints = new HashMap<>();
            String template = "SELECT c.column_name, c.data_type, tc.constraint_type FROM information_schema.table_constraints tc JOIN information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name) JOIN information_schema.columns AS c ON c.table_schema = tc.constraint_schema AND tc.table_name = c.table_name AND ccu.column_name = c.column_name WHERE (constraint_type = 'PRIMARY KEY' OR constraint_type = 'UNIQUE' OR constraint_type = 'EXCLUDE') AND c.table_name = ?;";
            QueryAdapter query = new QueryAdapter(template);
            ResultSet rs = query.executeAndGet(globalState, template, tableName);
            while (rs.next()) {
                String columnName = rs.getString("column_name");
                String dataType = rs.getString("data_type");
                String constraintType = rs.getString("constraint_type");
                // data types money & bit varying have no default operator class for specified partition method
                if (!(dataType.equals("money") || dataType.equals("bit varying"))) {
                    PostgresColumn c = new PostgresColumn(columnName, PostgresSchema.getColumnType(dataType));
                    if (columnConstraints.containsKey(c)) {
                        columnConstraints.get(c).add(constraintType);
                    } else {
                        columnConstraints.put(c, new ArrayList<>(Arrays.asList(constraintType)));
                    }
                }
            }
            for (PostgresColumn c : columnConstraints.keySet()) {
                // TODO: check if table and column constraint sets are equal? but then it's O(N) instead of O(1)
                if (tableConstraints.size() == columnConstraints.get(c).size()) {
                    columns.add(c);
                }
            }
            // TODO: figure out how to use EXCLUDE
        }
        distributeTable(columns, tableName, globalState, con);
    }

    @Override
    public void generateDatabase(PostgresGlobalState globalState) throws SQLException {
        // TODO: function reading? add to Postgres implementation?
        createTables(globalState);
        for (PostgresTable table : globalState.getSchema().getDatabaseTables()) {
            if (!(table.getTableType() == TableType.TEMPORARY || Randomly.getBooleanWithRatherLowProbability())) {
                if (Randomly.getBooleanWithRatherLowProbability()) {
                    // create reference table
                    String template = "SELECT create_reference_table(?);";
                    QueryAdapter query = new QueryAdapter(template, errors);
                    globalState.executeStatement(query, template, table.getName());
                } else {
                    // create distributed table
                    createDistributedTable(table.getName(), (CitusGlobalState) globalState,
                            globalState.getConnection());
                }
            }
            // else: keep local table
        }
        ((CitusGlobalState) globalState).updateSchema();
        prepareTables(globalState);
        if (((CitusGlobalState) globalState).getRepartition()) {
            // allow repartition joins
            globalState.executeStatement(new QueryAdapter("SET citus.enable_repartition_joins to ON;\n", errors));
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
            globalState.getState().logStatement(new QueryAdapter("CREATE EXTENSION citus;"));
            try (Statement s = con.createStatement()) {
                s.execute("CREATE EXTENSION citus;");
            }
            con.close();

            // reconnect to coordinator node, entry database
            globalState.getState().logStatement(String.format("\\c %s;", entryDatabaseName));
            con = DriverManager.getConnection("jdbc:" + entryURL, username, password);

            // read info about worker nodes
            globalState.getState().logStatement("SELECT * FROM master_get_active_worker_nodes()");
            List<WorkerNode> workerNodes = new ArrayList<>();
            try (Statement s = con.createStatement()) {
                ResultSet rs = s.executeQuery("SELECT * FROM master_get_active_worker_nodes();");
                while (rs.next()) {
                    String nodeHost = rs.getString("node_name");
                    int nodePort = rs.getInt("node_port");
                    WorkerNode w = new WorkerNode(nodeHost, nodePort);
                    workerNodes.add(w);
                }
            }
            con.close();

            for (WorkerNode w : workerNodes) {
                // connect to worker node, entry database
                int hostIndex = entryURL.indexOf(host);
                String preHost = entryURL.substring(0, hostIndex);
                String postHost = entryURL.substring(databaseIndex - 1);
                String entryWorkerURL = preHost + w.getHost() + ":" + w.getPort() + postHost;
                // TODO: better way of logging this
                globalState.getState().logStatement("\\q");
                globalState.getState().logStatement(entryWorkerURL);
                globalState.getState().logStatement(String.format("\\c %s;", entryDatabaseName));
                con = DriverManager.getConnection("jdbc:" + entryWorkerURL, username, password);

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
                String postDatabaseNameWorker = entryWorkerURL
                        .substring(databaseIndexWorker + entryDatabaseName.length());
                String testWorkerURL = preDatabaseNameWorker + databaseName + postDatabaseNameWorker;
                globalState.getState().logStatement(String.format("\\c %s;", databaseName));
                con = DriverManager.getConnection("jdbc:" + testWorkerURL, username, password);

                // add citus extension to worker node, test database
                globalState.getState().logStatement("CREATE EXTENSION citus;");
                try (Statement s = con.createStatement()) {
                    s.execute("CREATE EXTENSION citus;");
                }
                con.close();
            }

            // reconnect to coordinator node, test database
            // TODO: better way of logging this
            globalState.getState().logStatement("\\q");
            globalState.getState().logStatement(testURL);
            con = DriverManager.getConnection("jdbc:" + testURL, username, password);

            // add worker nodes to coordinator node for test database
            for (WorkerNode w : workerNodes) {
                // TODO: protect from sql injection - is it necessary though since these are read from the system?
                String addWorkers = "SELECT * from master_add_node('" + w.getHost() + "', " + w.getPort() + ");";
                globalState.getState().logStatement(addWorkers);
                try (Statement s = con.createStatement()) {
                    s.execute(addWorkers);
                }
            }
            con.close();
            // reconnect to coordinator node, test database
            con = DriverManager.getConnection("jdbc:" + testURL, username, password);
            return con;
        }
    }

    @Override
    protected void createTables(PostgresGlobalState globalState) throws SQLException {
        while (globalState.getSchema().getDatabaseTables().size() < Randomly.fromOptions(1, 2)) {
            try {
                String tableName = SQLite3Common.createTableName(globalState.getSchema().getDatabaseTables().size());
                Query createTable = CitusTableGenerator.generate(tableName, globalState.getSchema(), generateOnlyKnown,
                        globalState);
                globalState.executeStatement(createTable);
            } catch (IgnoreMeException e) {

            }
        }
    }

    @Override
    protected void prepareTables(PostgresGlobalState globalState) throws SQLException {
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

}
