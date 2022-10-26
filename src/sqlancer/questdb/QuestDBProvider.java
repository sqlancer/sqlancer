package sqlancer.questdb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import com.google.auto.service.AutoService;

import sqlancer.AbstractAction;
import sqlancer.DatabaseProvider;
import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.SQLGlobalState;
import sqlancer.SQLProviderAdapter;
import sqlancer.StatementExecutor;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLQueryProvider;
import sqlancer.questdb.QuestDBProvider.QuestDBGlobalState;
import sqlancer.questdb.gen.QuestDBAlterIndexGenerator;
import sqlancer.questdb.gen.QuestDBInsertGenerator;
import sqlancer.questdb.gen.QuestDBTableGenerator;
import sqlancer.questdb.gen.QuestDBTruncateGenerator;

@AutoService(DatabaseProvider.class)
public class QuestDBProvider extends SQLProviderAdapter<QuestDBGlobalState, QuestDBOptions> {
    public QuestDBProvider() {
        super(QuestDBGlobalState.class, QuestDBOptions.class);
    }

    public enum Action implements AbstractAction<QuestDBGlobalState> {
        INSERT(QuestDBInsertGenerator::getQuery), //
        ALTER_INDEX(QuestDBAlterIndexGenerator::getQuery), //
        TRUNCATE(QuestDBTruncateGenerator::generate); //
        // TODO (anxing): maybe implement these later
        // UPDATE(QuestDBUpdateGenerator::getQuery), //
        // CREATE_VIEW(QuestDBViewGenerator::generate), //

        private final SQLQueryProvider<QuestDBGlobalState> sqlQueryProvider;

        Action(SQLQueryProvider<QuestDBGlobalState> sqlQueryProvider) {
            this.sqlQueryProvider = sqlQueryProvider;
        }

        @Override
        public SQLQueryAdapter getQuery(QuestDBGlobalState state) throws Exception {
            return sqlQueryProvider.getQuery(state);
        }
    }

    private static int mapActions(QuestDBGlobalState globalState, Action a) {
        Randomly r = globalState.getRandomly();
        switch (a) {
        case INSERT:
            return r.getInteger(0, globalState.getOptions().getMaxNumberInserts());
        case ALTER_INDEX:
            return r.getInteger(0, 3);
        case TRUNCATE:
            return r.getInteger(0, 5);
        default:
            throw new AssertionError("Unknown action: " + a);
        }
    }

    public static class QuestDBGlobalState extends SQLGlobalState<QuestDBOptions, QuestDBSchema> {

        @Override
        protected QuestDBSchema readSchema() throws SQLException {
            return QuestDBSchema.fromConnection(getConnection(), getDatabaseName());
        }

    }

    @Override
    public void generateDatabase(QuestDBGlobalState globalState) throws Exception {
        for (int i = 0; i < Randomly.fromOptions(1, 2); i++) {
            boolean success;
            do {
                SQLQueryAdapter qt = new QuestDBTableGenerator().getQuery(globalState, null);
                success = globalState.executeStatement(qt);
            } while (!success);
        }
        if (globalState.getSchema().getDatabaseTables().isEmpty()) {
            throw new IgnoreMeException();
        }
        StatementExecutor<QuestDBGlobalState, Action> se = new StatementExecutor<>(globalState, Action.values(),
                QuestDBProvider::mapActions, (q) -> {
                    if (globalState.getSchema().getDatabaseTables().isEmpty()) {
                        throw new IgnoreMeException();
                    }
                });
        se.executeStatements();
    }

    @Override
    public SQLConnection createDatabase(QuestDBGlobalState globalState) throws Exception {
        String host = globalState.getOptions().getHost();
        int port = globalState.getOptions().getPort();
        if (host == null) {
            host = QuestDBOptions.DEFAULT_HOST;
        }
        if (port == sqlancer.MainOptions.NO_SET_PORT) {
            port = QuestDBOptions.DEFAULT_PORT;
        }
        // TODO(anxing): maybe not hardcode here...
        String databaseName = "qdb";
        String tableName = "test";
        String url = String.format("jdbc:postgresql://%s:%d/%s", host, port, databaseName);
        // use QuestDB default username & password for Postgres JDBC
        Properties properties = new Properties();
        properties.setProperty("user", globalState.getDbmsSpecificOptions().getUserName());
        properties.setProperty("password", globalState.getDbmsSpecificOptions().getPassword());
        properties.setProperty("sslmode", "disable");

        Connection con = DriverManager.getConnection(url, properties);
        // QuestDB cannot create or drop `DATABASE`, can only create or drop `TABLE`
        globalState.getState().logStatement("DROP TABLE IF EXISTS " + tableName + " CASCADE");
        SQLQueryAdapter createTableCommand = new QuestDBTableGenerator().getQuery(globalState, tableName);
        globalState.getState().logStatement(createTableCommand);
        globalState.getState().logStatement("DROP TABLE IF EXISTS " + tableName);

        try (Statement s = con.createStatement()) {
            s.execute("DROP TABLE IF EXISTS " + tableName);
        }
        // TODO(anxing): Drop all previous tables in db
        // List<String> tableNames =
        // globalState.getSchema().getDatabaseTables().stream().map(AbstractTable::getName).collect(Collectors.toList());
        // for (String tName : tableNames) {
        // try (Statement s = con.createStatement()) {
        // String query = "DROP TABLE IF EXISTS " + tName;
        // globalState.getState().logStatement(query);
        // s.execute(query);
        // }
        // }
        try (Statement s = con.createStatement()) {
            s.execute(createTableCommand.getQueryString());
        }
        // drop test table
        try (Statement s = con.createStatement()) {
            s.execute("DROP TABLE IF EXISTS " + tableName);
        }
        con.close();
        con = DriverManager.getConnection(url, properties);
        return new SQLConnection(con);
    }

    @Override
    public String getDBMSName() {
        return "questdb";
    }

}
