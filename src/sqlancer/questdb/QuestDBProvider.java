package sqlancer.questdb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

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
    private static final String JDBC_DRIVER_URL_FORMAT = "jdbc:postgresql://%s:%s/qdb";
    private static final String TEST_TABLE_NAME = "sqlancer_test";
    private static final String DROP_DATABASE_STMT = "DROP DATABASE";

    private static final AtomicBoolean HAS_CLEARED_DATABASE = new AtomicBoolean();

    private final QuestDBTableGenerator tableGenerator = new QuestDBTableGenerator();

    public QuestDBProvider() {
        super(QuestDBGlobalState.class, QuestDBOptions.class);
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

    @Override
    public void generateDatabase(QuestDBGlobalState globalState) throws Exception {
        for (int i = 0; i < Randomly.fromOptions(1, 2); i++) {
            boolean success;
            do {
                success = globalState.executeStatement(tableGenerator.getQuery(globalState, null));
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

        // use QuestDB default username & password for Postgres JDBC
        String url = String.format(JDBC_DRIVER_URL_FORMAT, host, port);
        Properties properties = new Properties();
        properties.setProperty("user", globalState.getDbmsSpecificOptions().getUserName());
        properties.setProperty("password", globalState.getDbmsSpecificOptions().getPassword());
        properties.setProperty("sslmode", "prefer");

        Connection con = DriverManager.getConnection(url, properties);

        if (HAS_CLEARED_DATABASE.compareAndSet(false, true)) {
            // drop database
            globalState.getState().logStatement(DROP_DATABASE_STMT);
            try (Statement s = con.createStatement()) {
                s.execute(DROP_DATABASE_STMT);
            }

            // create test table
            SQLQueryAdapter createTableCommand = tableGenerator.getQuery(globalState, TEST_TABLE_NAME);
            globalState.getState().logStatement(createTableCommand);
            try (Statement s = con.createStatement()) {
                s.execute(createTableCommand.getQueryString());
            }
        }
        return new SQLConnection(con);
    }

    @Override
    public String getDBMSName() {
        return "questdb";
    }

    public enum Action implements AbstractAction<QuestDBGlobalState> {
        INSERT(QuestDBInsertGenerator::getQuery), //
        ALTER_INDEX(QuestDBAlterIndexGenerator::getQuery), //
        TRUNCATE(QuestDBTruncateGenerator::getQuery); //
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

    public static class QuestDBGlobalState extends SQLGlobalState<QuestDBOptions, QuestDBSchema> {
        @Override
        protected QuestDBSchema readSchema() throws SQLException {
            return QuestDBSchema.fromConnection(getConnection());
        }
    }
}
