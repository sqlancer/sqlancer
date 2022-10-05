package sqlancer.yugabyte.ycql;

import static sqlancer.yugabyte.ycql.YCQLSchema.getTableNames;
import static sqlancer.yugabyte.ysql.YSQLProvider.DDL_LOCK;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

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
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLQueryProvider;
import sqlancer.yugabyte.ycql.YCQLProvider.YCQLGlobalState;
import sqlancer.yugabyte.ycql.gen.YCQLAlterTableGenerator;
import sqlancer.yugabyte.ycql.gen.YCQLDeleteGenerator;
import sqlancer.yugabyte.ycql.gen.YCQLIndexGenerator;
import sqlancer.yugabyte.ycql.gen.YCQLInsertGenerator;
import sqlancer.yugabyte.ycql.gen.YCQLRandomQuerySynthesizer;
import sqlancer.yugabyte.ycql.gen.YCQLTableGenerator;
import sqlancer.yugabyte.ycql.gen.YCQLUpdateGenerator;

@AutoService(DatabaseProvider.class)
public class YCQLProvider extends SQLProviderAdapter<YCQLGlobalState, YCQLOptions> {

    public YCQLProvider() {
        super(YCQLGlobalState.class, YCQLOptions.class);
    }

    public enum Action implements AbstractAction<YCQLGlobalState> {

        ALTER(YCQLAlterTableGenerator::getQuery), //
        INSERT(YCQLInsertGenerator::getQuery), //
        CREATE_INDEX(YCQLIndexGenerator::getQuery), //
        DELETE(YCQLDeleteGenerator::generate), //
        UPDATE(YCQLUpdateGenerator::getQuery), //
        EXPLAIN((g) -> {
            ExpectedErrors errors = new ExpectedErrors();
            YCQLErrors.addExpressionErrors(errors);
            return new SQLQueryAdapter(
                    "EXPLAIN " + YCQLToStringVisitor
                            .asString(YCQLRandomQuerySynthesizer.generateSelect(g, Randomly.smallNumber() + 1)),
                    errors);
        });

        private final SQLQueryProvider<YCQLGlobalState> sqlQueryProvider;

        Action(SQLQueryProvider<YCQLGlobalState> sqlQueryProvider) {
            this.sqlQueryProvider = sqlQueryProvider;
        }

        @Override
        public SQLQueryAdapter getQuery(YCQLGlobalState state) throws Exception {
            return sqlQueryProvider.getQuery(state);
        }
    }

    private static int mapActions(YCQLGlobalState globalState, Action a) {
        Randomly r = globalState.getRandomly();
        switch (a) {
        case ALTER:
            return r.getInteger(0, 10);
        case INSERT:
            return r.getInteger(0, globalState.getOptions().getMaxNumberInserts());
        case CREATE_INDEX:
        case UPDATE:
            return r.getInteger(0, globalState.getDbmsSpecificOptions().maxNumUpdates + 1);
        case EXPLAIN:
            return r.getInteger(0, 2);
        case DELETE:
            return r.getInteger(0, globalState.getDbmsSpecificOptions().maxNumDeletes + 1);
        default:
            throw new AssertionError(a);
        }
    }

    public static class YCQLGlobalState extends SQLGlobalState<YCQLOptions, YCQLSchema> {

        @Override
        protected YCQLSchema readSchema() throws SQLException {
            return YCQLSchema.fromConnection(getConnection(), getDatabaseName());
        }

    }

    @Override
    public void generateDatabase(YCQLGlobalState globalState) throws Exception {
        for (int i = 0; i < Randomly.fromOptions(1, 2); i++) {
            boolean success;
            do {
                SQLQueryAdapter qt = new YCQLTableGenerator().getQuery(globalState);
                success = globalState.executeStatement(qt);
            } while (!success);
        }
        if (globalState.getSchema().getDatabaseTables().isEmpty()) {
            throw new IgnoreMeException(); // TODO
        }
        StatementExecutor<YCQLGlobalState, Action> se = new StatementExecutor<>(globalState, Action.values(),
                YCQLProvider::mapActions, (q) -> {
                    if (globalState.getSchema().getDatabaseTables().isEmpty()) {
                        throw new IgnoreMeException();
                    }
                });
        se.executeStatements();
    }

    @Override
    public SQLConnection createDatabase(YCQLGlobalState globalState) throws SQLException {
        try {
            Class.forName("com.ing.data.cassandra.jdbc.CassandraDriver");
        } catch (ClassNotFoundException e) {
            throw new AssertionError();
        }

        String host = globalState.getOptions().getHost();
        int port = globalState.getOptions().getPort();

        if (host == null) {
            host = YCQLOptions.DEFAULT_HOST;
        }
        if (port == MainOptions.NO_SET_PORT) {
            port = YCQLOptions.DEFAULT_PORT;
        }

        final String url = "jdbc:cassandra://%s:%s/%s?localdatacenter=%s";
        final Connection connection = DriverManager.getConnection(
                String.format(url, host, port, "system_schema", globalState.getDbmsSpecificOptions().datacenter));

        synchronized (DDL_LOCK) {
            try (Statement stmt = connection.createStatement()) {
                try {
                    stmt.execute("DROP KEYSPACE IF EXISTS " + globalState.getDatabaseName());
                } catch (Exception se) {
                    // try again
                    List<String> tableNames = getTableNames(
                            new SQLConnection(DriverManager.getConnection(String.format(url, host, port,
                                    globalState.getDatabaseName(), globalState.getDbmsSpecificOptions().datacenter))),
                            globalState.getDatabaseName());
                    for (String tableName : tableNames) {
                        stmt.execute("DROP TABLE " + globalState.getDatabaseName() + "." + tableName);
                    }
                    stmt.execute("DROP KEYSPACE IF EXISTS " + globalState.getDatabaseName());
                }

                stmt.execute("CREATE KEYSPACE IF NOT EXISTS " + globalState.getDatabaseName());
            }
        }

        return new SQLConnection(DriverManager.getConnection(String.format(url, host, port,
                globalState.getDatabaseName(), globalState.getDbmsSpecificOptions().datacenter)));
    }

    @Override
    public String getDBMSName() {
        return "ycql";
    }

}
