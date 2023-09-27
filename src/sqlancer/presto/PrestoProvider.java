package sqlancer.presto;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import com.google.auto.service.AutoService;

import sqlancer.AbstractAction;
import sqlancer.DatabaseProvider;
import sqlancer.IgnoreMeException;
import sqlancer.MainOptions;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.SQLProviderAdapter;
import sqlancer.StatementExecutor;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLQueryProvider;
import sqlancer.presto.gen.PrestoInsertGenerator;
import sqlancer.presto.gen.PrestoTableGenerator;

@AutoService(DatabaseProvider.class)
public class PrestoProvider extends SQLProviderAdapter<PrestoGlobalState, PrestoOptions> {

    public PrestoProvider() {
        super(PrestoGlobalState.class, PrestoOptions.class);
    }

    // TODO : check actions based on connector
    // returns number of actions
    private static int mapActions(PrestoGlobalState globalState, Action a) {
        Randomly r = globalState.getRandomly();
        if (Objects.requireNonNull(a) == Action.INSERT) {
            return r.getInteger(0, globalState.getOptions().getMaxNumberInserts());
            // case UPDATE:
            // return r.getInteger(0, globalState.getDbmsSpecificOptions().maxNumUpdates + 1);
            // case EXPLAIN:
            // return r.getInteger(0, 2);
            // case DELETE:
            // return r.getInteger(0, globalState.getDbmsSpecificOptions().maxNumDeletes + 1);
            // case CREATE_VIEW:
            // return r.getInteger(0, globalState.getDbmsSpecificOptions().maxNumViews + 1);
        }
        throw new AssertionError(a);
    }

    @Override
    public void generateDatabase(PrestoGlobalState globalState) throws Exception {
        for (int i = 0; i < Randomly.fromOptions(1, 2); i++) {
            boolean success;
            do {
                SQLQueryAdapter qt = new PrestoTableGenerator().getQuery(globalState);
                success = globalState.executeStatement(qt);
            } while (!success);
        }
        if (globalState.getSchema().getDatabaseTables().isEmpty()) {
            throw new IgnoreMeException(); // TODO
        }
        StatementExecutor<PrestoGlobalState, Action> se = new StatementExecutor<>(globalState, Action.values(),
                PrestoProvider::mapActions, (q) -> {
                    if (globalState.getSchema().getDatabaseTables().isEmpty()) {
                        throw new IgnoreMeException();
                    }
                });
        se.executeStatements();
    }

    @Override
    public SQLConnection createDatabase(PrestoGlobalState globalState) throws SQLException {
        String username = globalState.getOptions().getUserName();
        String password = globalState.getOptions().getPassword();
        boolean useSSl = true;
        if (globalState.getOptions().isDefaultUsername() && globalState.getOptions().isDefaultPassword()) {
            username = "presto";
            password = null;
            useSSl = false;
        }
        String host = globalState.getOptions().getHost();
        int port = globalState.getOptions().getPort();
        if (host == null) {
            host = PrestoOptions.DEFAULT_HOST;
        }
        if (port == MainOptions.NO_SET_PORT) {
            port = PrestoOptions.DEFAULT_PORT;
        }
        String catalogName = globalState.getDbmsSpecificOptions().catalog;
        String databaseName = globalState.getDatabaseName();
        String url = String.format("jdbc:presto://%s:%d/%s?SSL=%b", host, port, catalogName, useSSl);
        Connection con = DriverManager.getConnection(url, username, password);
        List<String> schemaNames = getSchemaNames(con, catalogName, databaseName);
        dropExistingTables(con, catalogName, databaseName, schemaNames);
        dropSchema(globalState, con, catalogName, databaseName);
        createSchema(globalState, con, catalogName, databaseName);
        useSchema(globalState, con, catalogName, databaseName);
        return new SQLConnection(con);

    }

    private static void useSchema(PrestoGlobalState globalState, Connection con, String catalogName,
            String databaseName) throws SQLException {
        globalState.getState().logStatement("USE " + catalogName + "." + databaseName);
        try (Statement s = con.createStatement()) {
            s.execute("USE " + catalogName + "." + databaseName);
        }
    }

    private static void createSchema(PrestoGlobalState globalState, Connection con, String catalogName,
            String databaseName) throws SQLException {
        globalState.getState().logStatement("CREATE SCHEMA IF NOT EXISTS " + catalogName + "." + databaseName);
        try (Statement s = con.createStatement()) {
            s.execute("CREATE SCHEMA IF NOT EXISTS " + catalogName + "." + databaseName);
        }
    }

    private static void dropSchema(PrestoGlobalState globalState, Connection con, String catalogName,
            String databaseName) throws SQLException {
        globalState.getState().logStatement("DROP SCHEMA IF EXISTS " + catalogName + "." + databaseName);
        try (Statement s = con.createStatement()) {
            s.execute("DROP SCHEMA IF EXISTS " + catalogName + "." + databaseName);
        }
    }

    private static List<String> getSchemaNames(Connection con, String catalogName, String databaseName)
            throws SQLException {
        List<String> schemaNames = new ArrayList<>();
        final String showSchemasSql = "SHOW SCHEMAS FROM " + catalogName + " LIKE '" + databaseName + "'";
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery(showSchemasSql)) {
                while (rs.next()) {
                    schemaNames.add(rs.getString("Schema"));
                }
            }
        }
        return schemaNames;
    }

    private static void dropExistingTables(Connection con, String catalogName, String databaseName,
            List<String> schemaNames) throws SQLException {
        if (!schemaNames.isEmpty()) {
            List<String> tableNames = new ArrayList<>();
            try (Statement s = con.createStatement()) {
                try (ResultSet rs = s.executeQuery("SHOW TABLES FROM " + catalogName + "." + databaseName)) {
                    while (rs.next()) {
                        tableNames.add(rs.getString("Table"));
                    }
                }
            }
            try (Statement s = con.createStatement()) {
                for (String tableName : tableNames) {
                    s.execute("DROP TABLE IF EXISTS " + catalogName + "." + databaseName + "." + tableName);
                }
            }
        }
    }

    @Override
    public String getDBMSName() {
        return "presto";
    }

    public enum Action implements AbstractAction<PrestoGlobalState> {
        // SHOW_TABLES((g) -> new SQLQueryAdapter("SHOW TABLES", new ExpectedErrors(), false, false)), //
        INSERT(PrestoInsertGenerator::getQuery);
        // TODO : check actions based on connector
        // DELETE(PrestoDeleteGenerator::generate), //
        // UPDATE(PrestoUpdateGenerator::getQuery), //
        // CREATE_VIEW(PrestoViewGenerator::generate), //
        // EXPLAIN((g) -> {
        // ExpectedErrors errors = new ExpectedErrors();
        // PrestoErrors.addExpressionErrors(errors);
        // PrestoErrors.addGroupByErrors(errors);
        // return new SQLQueryAdapter(
        // "EXPLAIN " + PrestoToStringVisitor
        // .asString(PrestoRandomQuerySynthesizer.generateSelect(g, Randomly.smallNumber() + 1)),
        // errors);
        // });

        private final SQLQueryProvider<PrestoGlobalState> sqlQueryProvider;

        Action(SQLQueryProvider<PrestoGlobalState> sqlQueryProvider) {
            this.sqlQueryProvider = sqlQueryProvider;
        }

        @Override
        public SQLQueryAdapter getQuery(PrestoGlobalState state) throws Exception {
            return sqlQueryProvider.getQuery(state);
        }
    }

}
