package sqlancer.spark;

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
import sqlancer.spark.gen.SparkInsertGenerator;
import sqlancer.spark.gen.SparkTableGenerator;

@AutoService(DatabaseProvider.class)
public class SparkProvider extends SQLProviderAdapter<SparkGlobalState, SparkOptions> {

    public SparkProvider() {
        super(SparkGlobalState.class, SparkOptions.class);
    }

    public enum Action implements AbstractAction<SparkGlobalState> {
        INSERT(SparkInsertGenerator::getQuery);

        private final SQLQueryProvider<SparkGlobalState> sqlQueryProvider;

        Action(SQLQueryProvider<SparkGlobalState> sqlQueryProvider) {
            this.sqlQueryProvider = sqlQueryProvider;
        }

        @Override
        public SQLQueryAdapter getQuery(SparkGlobalState state) throws Exception {
            return sqlQueryProvider.getQuery(state);
        }
    }

    private static int mapActions(SparkGlobalState globalState, Action a) {
        Randomly r = globalState.getRandomly();
        switch (a) {
        case INSERT:
            return r.getInteger(0, globalState.getOptions().getMaxNumberInserts());
        default:
            throw new AssertionError(a);
        }
    }

    @Override
    public void generateDatabase(SparkGlobalState globalState) throws Exception {
        for (int i = 0; i < Randomly.fromOptions(1, 2); i++) {
            boolean success;
            do {
                String tableName = globalState.getSchema().getFreeTableName();
                SQLQueryAdapter qt = SparkTableGenerator.generate(globalState, tableName);
                success = globalState.executeStatement(qt);
            } while (!success);
        }

        if (globalState.getSchema().getDatabaseTables().isEmpty()) {
            throw new IgnoreMeException();
        }

        StatementExecutor<SparkGlobalState, Action> se = new StatementExecutor<>(globalState, Action.values(),
                SparkProvider::mapActions, (q) -> {
                    if (globalState.getSchema().getDatabaseTables().isEmpty()) {
                        throw new IgnoreMeException();
                    }
                });
        se.executeStatements();
    }

    @Override
    public SQLConnection createDatabase(SparkGlobalState globalState) throws SQLException {
        String username = globalState.getOptions().getUserName();
        String password = globalState.getOptions().getPassword();
        String host = globalState.getOptions().getHost();
        int port = globalState.getOptions().getPort();

        if (host == null) {
            host = SparkOptions.DEFAULT_HOST;
        }
        if (port == MainOptions.NO_SET_PORT) {
            port = SparkOptions.DEFAULT_PORT;
        }

        String databaseName = globalState.getDatabaseName();

        // Spark uses the Hive driver for JDBC usually
        String url = String.format("jdbc:hive2://%s:%d/%s", host, port, "default");

        // Connect to default to create the fuzzing DB
        Connection con = DriverManager.getConnection(url, username, password);
        try (Statement s = con.createStatement()) {
            s.execute("DROP DATABASE IF EXISTS " + databaseName + " CASCADE");
        }
        try (Statement s = con.createStatement()) {
            s.execute("CREATE DATABASE " + databaseName);
        }
        con.close();

        // Connect to the specific fuzzing DB
        con = DriverManager.getConnection(String.format("jdbc:hive2://%s:%d/%s", host, port, databaseName), username,
                password);
        try (Statement s = con.createStatement()) {
            // This allows casting things like BOOLEAN to DATE/TIMESTAMP, which the
            // generator loves to do.
            s.execute("SET spark.sql.ansi.enabled=false");
        }
        return new SQLConnection(con);
    }

    @Override
    public String getDBMSName() {
        return "spark";
    }
}
