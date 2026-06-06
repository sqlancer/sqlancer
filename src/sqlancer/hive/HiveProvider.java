package sqlancer.hive;

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
import sqlancer.SQLProviderAdapter;
import sqlancer.StatementExecutor;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLQueryProvider;
import sqlancer.hive.gen.HiveInsertGenerator;
import sqlancer.hive.gen.HiveTableGenerator;

@AutoService(DatabaseProvider.class)
public class HiveProvider extends SQLProviderAdapter<HiveGlobalState, HiveOptions> {

    public HiveProvider() {
        super(HiveGlobalState.class, HiveOptions.class);
    }

    public enum Action implements AbstractAction<HiveGlobalState> {

        INSERT(HiveInsertGenerator::getQuery);

        private final SQLQueryProvider<HiveGlobalState> sqlQueryProvider;

        Action(SQLQueryProvider<HiveGlobalState> sqlQueryProvider) {
            this.sqlQueryProvider = sqlQueryProvider;
        }

        @Override
        public SQLQueryAdapter getQuery(HiveGlobalState state) throws Exception {
            return sqlQueryProvider.getQuery(state);
        }
    }

    private static int mapActions(HiveGlobalState globalState, Action a) {
        Randomly r = globalState.getRandomly();
        switch (a) {
        case INSERT:
            return r.getInteger(0, globalState.getOptions().getMaxNumberInserts());
        default:
            throw new AssertionError(a);
        }
    }

    @Override
    public void generateDatabase(HiveGlobalState globalState) throws Exception {
        for (int i = 0; i < Randomly.fromOptions(1, 2); i++) {
            boolean success;
            do {
                String tableName = globalState.getSchema().getFreeTableName();
                SQLQueryAdapter qt = HiveTableGenerator.generate(globalState, tableName);
                success = globalState.executeStatement(qt);
            } while (!success);
        }
        if (globalState.getSchema().getDatabaseTables().isEmpty()) {
            throw new IgnoreMeException(); // TODO
        }

        StatementExecutor<HiveGlobalState, Action> se = new StatementExecutor<>(globalState, Action.values(),
                HiveProvider::mapActions, (q) -> {
                    if (globalState.getSchema().getDatabaseTables().isEmpty()) {
                        throw new IgnoreMeException();
                    }
                });
        se.executeStatements();
    }

    @Override
    public SQLConnection createDatabase(HiveGlobalState globalState) throws SQLException {
        String username = globalState.getOptions().getUserName();
        String password = globalState.getOptions().getPassword();
        String host = globalState.getOptions().getHost();
        int port = globalState.getOptions().getPort();
        if (host == null) {
            host = HiveOptions.DEFAULT_HOST;
        }
        if (port == MainOptions.NO_SET_PORT) {
            port = HiveOptions.DEFAULT_PORT;
        }

        String databaseName = globalState.getDatabaseName();

        String url = String.format("jdbc:hive2://%s:%d/%s", host, port, "default");
        Connection con = DriverManager.getConnection(url, username, password);
        globalState.getState().logStatement("DROP DATABASE IF EXISTS " + databaseName + " CASCADE");
        globalState.getState().logStatement("CREATE DATABASE " + databaseName);
        globalState.getState().logStatement("USE " + databaseName);
        try (Statement s = con.createStatement()) {
            s.execute("DROP DATABASE IF EXISTS " + databaseName + " CASCADE");
        }
        try (Statement s = con.createStatement()) {
            s.execute("CREATE DATABASE " + databaseName);
        }
        try (Statement s = con.createStatement()) {
            s.execute("USE " + databaseName);
        }
        con.close();
        con = DriverManager
                .getConnection(String.format("jdbc:hive2://%s:%d/%s", host, port, databaseName, username, password));

        return new SQLConnection(con);
    }

    @Override
    public String getDBMSName() {
        return "hive";
    }
}
