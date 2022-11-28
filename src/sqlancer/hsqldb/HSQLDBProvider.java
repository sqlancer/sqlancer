package sqlancer.hsqldb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

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
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLQueryProvider;
import sqlancer.hsqldb.gen.HSQLDBInsertGenerator;
import sqlancer.hsqldb.gen.HSQLDBTableGenerator;
import sqlancer.hsqldb.gen.HSQLDBUpdateGenerator;

@AutoService(DatabaseProvider.class)
public class HSQLDBProvider extends SQLProviderAdapter<HSQLDBProvider.HSQLDBGlobalState, HSQLDBOptions> {

    private static final String HSQLDB = "hsqldb";

    public HSQLDBProvider() {
        super(HSQLDBGlobalState.class, HSQLDBOptions.class);
    }

    public enum Action implements AbstractAction<HSQLDBGlobalState> {
        INSERT(HSQLDBInsertGenerator::getQuery), UPDATE(HSQLDBUpdateGenerator::getQuery);

        private final SQLQueryProvider<HSQLDBProvider.HSQLDBGlobalState> sqlQueryProvider;

        Action(SQLQueryProvider<HSQLDBProvider.HSQLDBGlobalState> sqlQueryProvider) {
            this.sqlQueryProvider = sqlQueryProvider;
        }

        @Override
        public SQLQueryAdapter getQuery(HSQLDBProvider.HSQLDBGlobalState state) throws Exception {
            return sqlQueryProvider.getQuery(state);
        }
    }

    @Override
    public SQLConnection createDatabase(HSQLDBGlobalState globalState) throws Exception {
        String databaseName = globalState.getDatabaseName();
        String url = "jdbc:hsqldb:file:" + databaseName;
        MainOptions options = globalState.getOptions();
        Connection connection = DriverManager.getConnection(url, options.getUserName(), options.getPassword());
        // When a server instance is started, or when a connection is made to an in-process database,
        // a new, empty database is created if no database exists at the given path.
        try (Statement s = connection.createStatement()) {
            s.execute("DROP SCHEMA PUBLIC CASCADE");
            s.execute("SET DATABASE SQL DOUBLE NAN FALSE");
        }
        return new SQLConnection(connection);
    }

    @Override
    public String getDBMSName() {
        return HSQLDB;
    }

    @Override
    public void generateDatabase(HSQLDBGlobalState globalState) throws Exception {
        for (int i = 0; i < Randomly.fromOptions(1, 2); i++) {
            boolean success;
            do {
                SQLQueryAdapter qt = new HSQLDBTableGenerator().getQuery(globalState, null);
                success = globalState.executeStatement(qt);
            } while (!success);
        }
        if (globalState.getSchema().getDatabaseTables().isEmpty()) {
            throw new IgnoreMeException();
        }
        StatementExecutor<HSQLDBGlobalState, Action> se = new StatementExecutor<>(globalState,
                HSQLDBProvider.Action.values(), HSQLDBProvider::mapActions, (q) -> {
                    if (globalState.getSchema().getDatabaseTables().isEmpty()) {
                        throw new IgnoreMeException();
                    }
                });
        se.executeStatements();
    }

    private static int mapActions(HSQLDBProvider.HSQLDBGlobalState globalState, HSQLDBProvider.Action a) {
        Randomly r = globalState.getRandomly();
        switch (a) {
        case INSERT:
            return r.getInteger(0, globalState.getOptions().getMaxNumberInserts());
        case UPDATE:
            return r.getInteger(0, 10);
        default:
            throw new AssertionError(a);
        }
    }

    public static class HSQLDBGlobalState extends SQLGlobalState<HSQLDBOptions, HSQLDBSchema> {

        @Override
        protected HSQLDBSchema readSchema() throws SQLException {
            return HSQLDBSchema.fromConnection(getConnection(), getDatabaseName());
        }

    }
}
