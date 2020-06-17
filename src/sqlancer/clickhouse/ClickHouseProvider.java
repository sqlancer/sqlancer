package sqlancer.clickhouse;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.AbstractAction;
import sqlancer.CompositeTestOracle;
import sqlancer.GlobalState;
import sqlancer.IgnoreMeException;
import sqlancer.Main.QueryManager;
import sqlancer.Main.StateLogger;
import sqlancer.ProviderAdapter;
import sqlancer.Query;
import sqlancer.QueryAdapter;
import sqlancer.QueryProvider;
import sqlancer.Randomly;
import sqlancer.StatementExecutor;
import sqlancer.TestOracle;
import sqlancer.clickhouse.ClickHouseProvider.ClickHouseGlobalState;
import sqlancer.clickhouse.gen.ClickHouseInsertGenerator;
import sqlancer.clickhouse.gen.ClickHouseTableGenerator;

public class ClickHouseProvider extends ProviderAdapter<ClickHouseGlobalState, ClickHouseOptions> {

    public ClickHouseProvider() {
        super(ClickHouseGlobalState.class, ClickHouseOptions.class);
    }

    public enum Action implements AbstractAction<ClickHouseGlobalState> {

        INSERT(ClickHouseInsertGenerator::getQuery);

        private final QueryProvider<ClickHouseGlobalState> queryProvider;

        Action(QueryProvider<ClickHouseGlobalState> queryProvider) {
            this.queryProvider = queryProvider;
        }

        @Override
        public Query getQuery(ClickHouseGlobalState state) throws SQLException {
            return queryProvider.getQuery(state);
        }
    }

    private static int mapActions(ClickHouseGlobalState globalState, Action a) {
        Randomly r = globalState.getRandomly();
        switch (a) {
        case INSERT:
            return r.getInteger(0, globalState.getOptions().getMaxNumberInserts());
        default:
            throw new AssertionError(a);
        }
    }

    public static class ClickHouseGlobalState extends GlobalState<ClickHouseOptions> {

        private ClickHouseSchema schema;
        private ClickHouseOptions clickHouseOptions;

        public void setSchema(ClickHouseSchema schema) {
            this.schema = schema;
        }

        public ClickHouseSchema getSchema() {
            return schema;
        }

        public void setClickHouseOptions(ClickHouseOptions clickHouseOptions) {
            this.clickHouseOptions = clickHouseOptions;
        }

        public ClickHouseOptions getClickHouseOptions() {
            return this.clickHouseOptions;
        }

        public String getOracleName() {
            return String.join("_",
                    this.clickHouseOptions.oracle.stream().map(o -> o.toString()).collect(Collectors.toList()));
        }

        @Override
        public String getDatabaseName() {
            return super.getDatabaseName() + this.getOracleName();
        }
    }

    @Override
    public void generateAndTestDatabase(ClickHouseGlobalState globalState) throws SQLException {
        StateLogger logger = globalState.getLogger();
        QueryManager manager = globalState.getManager();
        globalState
                .setSchema(ClickHouseSchema.fromConnection(globalState.getConnection(), globalState.getDatabaseName()));
        for (int i = 0; i < Randomly.fromOptions(1); i++) {
            boolean success = false;
            do {
                Query qt = new ClickHouseTableGenerator().getQuery(globalState);
                success = manager.execute(qt);
                logger.writeCurrent(globalState.getState());
                globalState.setSchema(
                        ClickHouseSchema.fromConnection(globalState.getConnection(), globalState.getDatabaseName()));
                try {
                    logger.getCurrentFileWriter().close();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                logger.currentFileWriter = null;
            } while (!success);
        }

        StatementExecutor<ClickHouseGlobalState, Action> se = new StatementExecutor<>(globalState, Action.values(),
                ClickHouseProvider::mapActions, (q) -> {
                    if (q.couldAffectSchema()) {
                        globalState.setSchema(ClickHouseSchema.fromConnection(globalState.getConnection(),
                                globalState.getDatabaseName()));
                    }
                    if (globalState.getSchema().getDatabaseTables().isEmpty()) {
                        throw new IgnoreMeException();
                    }
                });
        se.executeStatements();
        manager.incrementCreateDatabase();

        List<TestOracle> oracles = globalState.getDmbsSpecificOptions().oracle.stream().map(o -> {
            try {
                return o.create(globalState);
            } catch (SQLException e1) {
                throw new AssertionError(e1);
            }
        }).collect(Collectors.toList());
        CompositeTestOracle oracle = new CompositeTestOracle(oracles);

        for (int i = 0; i < globalState.getOptions().getNrQueries(); i++) {
            try {
                oracle.check();
            } catch (IgnoreMeException e) {
                continue;
            }
            manager.incrementSelectQueryCount();
        }

        try {
            if (globalState.getOptions().logEachSelect()) {
                logger.getCurrentFileWriter().close();
                logger.currentFileWriter = null;
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Override
    public Connection createDatabase(ClickHouseGlobalState globalState) throws SQLException {
        ClickHouseOptions clickHouseOptions = globalState.getDmbsSpecificOptions();
        globalState.setClickHouseOptions(clickHouseOptions);
        String url = "jdbc:clickhouse://localhost:8123/test";
        String databaseName = globalState.getDatabaseName();
        Connection con = DriverManager.getConnection(url, globalState.getOptions().getUserName(),
                globalState.getOptions().getPassword());
        String dropDatabaseCommand = "DROP DATABASE IF EXISTS " + databaseName;
        globalState.getState().statements.add(new QueryAdapter(dropDatabaseCommand));
        String createDatabaseCommand = "CREATE DATABASE IF NOT EXISTS " + databaseName;
        globalState.getState().statements.add(new QueryAdapter(createDatabaseCommand));
        try (Statement s = con.createStatement()) {
            s.execute(dropDatabaseCommand);
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        try (Statement s = con.createStatement()) {
            s.execute(createDatabaseCommand);
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        con.close();
        con = DriverManager.getConnection("jdbc:clickhouse://localhost:18123/" + databaseName,
                globalState.getOptions().getUserName(), globalState.getOptions().getPassword());
        return con;
    }

    @Override
    public String getDBMSName() {
        return "clickhouse";
    }
}
