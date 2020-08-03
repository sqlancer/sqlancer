package sqlancer.clickhouse;

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
import sqlancer.ProviderAdapter;
import sqlancer.Query;
import sqlancer.QueryProvider;
import sqlancer.Randomly;
import sqlancer.StatementExecutor;
import sqlancer.TestOracle;
import sqlancer.clickhouse.ClickHouseProvider.ClickHouseGlobalState;
import sqlancer.clickhouse.gen.ClickHouseCommon;
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

    public static class ClickHouseGlobalState extends GlobalState<ClickHouseOptions, ClickHouseSchema> {

        private ClickHouseOptions clickHouseOptions;

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

        @Override
        protected void updateSchema() throws SQLException {
            setSchema(ClickHouseSchema.fromConnection(getConnection(), getDatabaseName()));
        }
    }

    @Override
    public void generateDatabase(ClickHouseGlobalState globalState) throws SQLException {
        for (int i = 0; i < Randomly.fromOptions(1); i++) {
            boolean success;
            do {
                String tableName = ClickHouseCommon.createTableName(i);
                Query qt = ClickHouseTableGenerator.createTableStatement(tableName, globalState);
                success = globalState.executeStatement(qt);
            } while (!success);
        }

        StatementExecutor<ClickHouseGlobalState, Action> se = new StatementExecutor<>(globalState, Action.values(),
                ClickHouseProvider::mapActions, (q) -> {
                    if (globalState.getSchema().getDatabaseTables().isEmpty()) {
                        throw new IgnoreMeException();
                    }
                });
        se.executeStatements();
    }

    @Override
    protected TestOracle getTestOracle(ClickHouseGlobalState globalState) throws SQLException {
        List<TestOracle> oracles = globalState.getDmbsSpecificOptions().oracle.stream().map(o -> {
            try {
                return o.create(globalState);
            } catch (SQLException e1) {
                throw new AssertionError(e1);
            }
        }).collect(Collectors.toList());
        return new CompositeTestOracle(oracles, globalState);
    }

    @Override
    public Connection createDatabase(ClickHouseGlobalState globalState) throws SQLException {
        ClickHouseOptions clickHouseOptions = globalState.getDmbsSpecificOptions();
        globalState.setClickHouseOptions(clickHouseOptions);
        String url = "jdbc:clickhouse://localhost:8123/default";
        String databaseName = globalState.getDatabaseName();
        Connection con = DriverManager.getConnection(url, globalState.getOptions().getUserName(),
                globalState.getOptions().getPassword());
        String dropDatabaseCommand = "DROP DATABASE IF EXISTS " + databaseName;
        globalState.getState().logStatement(dropDatabaseCommand);
        String createDatabaseCommand = "CREATE DATABASE IF NOT EXISTS " + databaseName;
        globalState.getState().logStatement(createDatabaseCommand);
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
        con = DriverManager.getConnection("jdbc:clickhouse://localhost:8123/" + databaseName,
                globalState.getOptions().getUserName(), globalState.getOptions().getPassword());
        return con;
    }

    @Override
    public String getDBMSName() {
        return "clickhouse";
    }
}
