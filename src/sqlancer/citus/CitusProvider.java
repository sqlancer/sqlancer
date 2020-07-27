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

import sqlancer.QueryAdapter;
import sqlancer.Randomly;
import sqlancer.citus.gen.CitusCommon;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresOptions;
import sqlancer.postgres.PostgresProvider;
import sqlancer.postgres.PostgresSchema;
import sqlancer.postgres.PostgresSchema.PostgresColumn;
import sqlancer.postgres.PostgresSchema.PostgresTable;

public class CitusProvider extends PostgresProvider {

    private static final Set<String> errors = new HashSet<>();
    
    
    @SuppressWarnings("unchecked")
    public CitusProvider() {
        super((Class<PostgresGlobalState>)(Object) CitusGlobalState.class, (Class<PostgresOptions>)(Object) CitusOptions.class);
        CitusCommon.addCitusErrors(errors);
    }

    private class WorkerNode{

        private final String host;
        private final int port;

        public WorkerNode(String node_host, int node_port) {
            this.host = node_host;
            this.port = node_port; 
        }

        public String get_host() {
            return this.host;
        }

        public int get_port() {
            return this.port;
        }

    }

    private static void distributeTable(List<PostgresColumn> columns, String tableName, CitusGlobalState globalState, Connection con) throws SQLException {
        if (columns.size() != 0) {
            PostgresColumn columnToDistribute = Randomly.fromList(columns);
            QueryAdapter query = new QueryAdapter("SELECT create_distributed_table('" + tableName + "', '" + columnToDistribute.getName() + "');", errors);
            String template = "SELECT create_distributed_table(?, ?);";
            List<String> fills = Arrays.asList(tableName, columnToDistribute.getName());
            globalState.fillAndExecuteStatement(query, template, fills);
            // distribution column cannot take NULL value
            // TODO: find a way to protect from SQL injection without '' around string input
            query = new QueryAdapter("ALTER TABLE " + tableName + " ALTER COLUMN " + columnToDistribute.getName() + " SET NOT NULL;", errors);
            globalState.executeStatement(query);
        }
    }

    private static List<String> getTableConstraints(String tableName, CitusGlobalState globalState, Connection con) throws SQLException {
        List<String> constraints = new ArrayList<>();
        QueryAdapter query = new QueryAdapter("SELECT constraint_type FROM information_schema.table_constraints WHERE table_name = '" + tableName + "' AND (constraint_type = 'PRIMARY KEY' OR constraint_type = 'UNIQUE' or constraint_type = 'EXCLUDE');");
        String template = "SELECT constraint_type FROM information_schema.table_constraints WHERE table_name = ? AND (constraint_type = 'PRIMARY KEY' OR constraint_type = 'UNIQUE' or constraint_type = 'EXCLUDE');";
        List<String> fills = new ArrayList<>();
        fills.add(tableName);
        ResultSet rs = query.fillAndExecuteAndGet(globalState, template, fills);
        while (rs.next()) {
            constraints.add(rs.getString("constraint_type"));
        }
        return constraints;
    }

    private static void createDistributedTable(String tableName, CitusGlobalState globalState, Connection con) throws SQLException {
        List<PostgresColumn> columns = new ArrayList<>();
        List<String> tableConstraints = getTableConstraints(tableName, globalState, con);
        if (tableConstraints.size() == 0) {
            QueryAdapter query = new QueryAdapter("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '" + tableName + "';");
            String template = "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = ?;";
            List<String> fills = Arrays.asList(tableName);
            ResultSet rs = query.fillAndExecuteAndGet(globalState, template, fills);
            while (rs.next()) {
                String columnName = rs.getString("column_name");
                String dataType = rs.getString("data_type");
                // data types money & bit varying have no default operator class for specified partition method
                if (! (dataType.equals("money") || dataType.equals("bit varying"))) {
                    PostgresColumn c = new PostgresColumn(columnName, PostgresSchema.getColumnType(dataType));
                    columns.add(c);
                }
            }
        } else {
            // TODO: multiple constraints?
            HashMap<PostgresColumn, List<String>> columnConstraints = new HashMap<>();
            QueryAdapter query = new QueryAdapter("SELECT c.column_name, c.data_type, tc.constraint_type FROM information_schema.table_constraints tc JOIN information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name) JOIN information_schema.columns AS c ON c.table_schema = tc.constraint_schema AND tc.table_name = c.table_name AND ccu.column_name = c.column_name WHERE (constraint_type = 'PRIMARY KEY' OR constraint_type = 'UNIQUE' OR constraint_type = 'EXCLUDE') AND c.table_name = '" + tableName + "';");
            // TODO: decide whether to log
            // globalState.getState().statements.add(query);
            String template = "SELECT c.column_name, c.data_type, tc.constraint_type FROM information_schema.table_constraints tc JOIN information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name) JOIN information_schema.columns AS c ON c.table_schema = tc.constraint_schema AND tc.table_name = c.table_name AND ccu.column_name = c.column_name WHERE (constraint_type = 'PRIMARY KEY' OR constraint_type = 'UNIQUE' OR constraint_type = 'EXCLUDE') AND c.table_name = ?;";
            List<String> fills = Arrays.asList(tableName);
            ResultSet rs = query.fillAndExecuteAndGet(globalState, template, fills);
            while (rs.next()) {
                String columnName = rs.getString("column_name");
                String dataType = rs.getString("data_type");
                String constraintType = rs.getString("constraint_type");
                // data types money & bit varying have no default operator class for specified partition method
                if (! (dataType.equals("money") || dataType.equals("bit varying"))) {
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
            // TODO: random 0-1 range double
            if (Randomly.getBooleanWithRatherLowProbability()) {
                // create local table
            } else if (Randomly.getBooleanWithRatherLowProbability()) {
                // create reference table
                QueryAdapter query = new QueryAdapter("SELECT create_reference_table('" + table.getName() + "');", errors);
                String template = "SELECT create_reference_table(?);";
                List<String> fills = Arrays.asList(table.getName());
                globalState.fillAndExecuteStatement(query, template, fills);
            } else {
                // create distributed table
                createDistributedTable(table.getName(), (CitusGlobalState) globalState, globalState.getConnection());
            }
        }
        ((CitusGlobalState) globalState).updateSchema();
        prepareTables(globalState);
        if (((CitusGlobalState) globalState).getRepartition()) {
            // allow repartition joins
            globalState.executeStatement(new QueryAdapter("SET citus.enable_repartition_joins to ON;\n", errors));
        }
    }

    //FIXME: pass in Postgres or CitusGlobalState?
    @Override
    public Connection createDatabase(PostgresGlobalState globalState) throws SQLException {
        synchronized(CitusProvider.class) {
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
                    String node_host = rs.getString("node_name");
                    int node_port = rs.getInt("node_port");
                    WorkerNode w = new WorkerNode(node_host, node_port);
                    workerNodes.add(w);
                }
            }
            con.close();

            for (WorkerNode w : workerNodes) {
                // connect to worker node, entry database
                int hostIndex = entryURL.indexOf(host);
                String preHost = entryURL.substring(0, hostIndex);
                String postHost = entryURL.substring(databaseIndex - 1);
                String entryWorkerURL = preHost + w.get_host() + ":" + w.get_port() + postHost;
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
                String postDatabaseNameWorker = entryWorkerURL.substring(databaseIndexWorker + entryDatabaseName.length());
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
                String addWorkers = "SELECT * from master_add_node('" + w.get_host() + "', " + w.get_port() + ");";
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
    public String getDBMSName() {
        return "citus";
    }

}