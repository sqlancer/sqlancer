package sqlancer.citus;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import sqlancer.citus.gen.CitusCommon;
import sqlancer.postgres.PostgresProvider;

public class CitusProvider extends PostgresProvider {
    
    protected final Set<String> errors = new HashSet<>();
    
    public CitusProvider() {
        super();
        CitusCommon.addCitusErrors(errors);
    }

    // FIXME: static or not?
    private class WorkerNode{

        private final String name;
        private final int port;

        public WorkerNode(String node_name, int node_port) {
            this.name = node_name;
            this.port = node_port; 
        }

        public String get_name() {
            return this.name;
        }

        public int get_port() {
            return this.port;
        }

    }

    // FIXME: static or not?
    private final void distributeTable(List<PostgresColumn> columns, String tableName, PostgresGlobalState globalState, Connection con) throws SQLException {
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

    private final List<String> getTableConstraints(String tableName, PostgresGlobalState globalState, Connection con) throws SQLException {
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

    // FIXME: static or not?
    private final void createDistributedTable(String tableName, PostgresGlobalState globalState, Connection con) throws SQLException {
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
                    PostgresColumn c = new PostgresColumn(columnName, getColumnType(dataType));
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
                    PostgresColumn c = new PostgresColumn(columnName, getColumnType(dataType));
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
                query = new QueryAdapter("SELECT create_reference_table('" + table.getName() + "');", errors);
                String template = "SELECT create_reference_table(?);";
                List<String> fills = Arrays.asList(table.getName());
                globalState.fillAndExecuteStatement(query, template, fills);
            } else {
                // create distributed table
                createDistributedTable(table.getName(), globalState, globalState.getConnection());
            }
        }
        globalState.updateSchema();
        prepareTables(globalState);
        if (globalState.getRepartition()) {
            // allow repartition joins
            globalState.executeStatement(new QueryAdapter("SET citus.enable_repartition_joins to ON;\n", errors));
        }
    }

}