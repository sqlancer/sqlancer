package sqlancer.tidb;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

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
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.tidb.TiDBProvider.TiDBGlobalState;
import sqlancer.tidb.gen.TiDBAlterTableGenerator;
import sqlancer.tidb.gen.TiDBAnalyzeTableGenerator;
import sqlancer.tidb.gen.TiDBDeleteGenerator;
import sqlancer.tidb.gen.TiDBDropTableGenerator;
import sqlancer.tidb.gen.TiDBDropViewGenerator;
import sqlancer.tidb.gen.TiDBIndexGenerator;
import sqlancer.tidb.gen.TiDBInsertGenerator;
import sqlancer.tidb.gen.TiDBSetGenerator;
import sqlancer.tidb.gen.TiDBTableGenerator;
import sqlancer.tidb.gen.TiDBUpdateGenerator;
import sqlancer.tidb.gen.TiDBViewGenerator;

@AutoService(DatabaseProvider.class)
public class TiDBProvider extends SQLProviderAdapter<TiDBGlobalState, TiDBOptions> {
    private HashMap<String, String> queryPlanPool;

    // Data structures for MAB algorithm at table mutation
    private double[] weightedAverageReward;
    private int[] cumulativeMutationTimes;

    // For post reward calculation
    private int currentSelectRewards;
    private int currentSelectCounts;
    private int currentMutationOperator;

    public static final int MAX_INDEXES = 20; // The maximum number of indexes to be created
    public static final int MAX_TABLES = 10; // The maximum number of tables/virtual tables/ rtree tables/ views to be
                                             // created

    public TiDBProvider() {
        super(TiDBGlobalState.class, TiDBOptions.class);
        queryPlanPool = new HashMap<>();
        weightedAverageReward = new double[Action.values().length];
        cumulativeMutationTimes = new int[Action.values().length];

        // For post reward calculation
        currentSelectRewards = 0;
        currentSelectCounts = 0;
        currentMutationOperator = -1;
    }

    public enum Action implements AbstractAction<TiDBGlobalState> {
        CREATE_TABLE(TiDBTableGenerator::createRandomTableStatement), // 0
        CREATE_INDEX(TiDBIndexGenerator::getQuery), // 1
        VIEW_GENERATOR(TiDBViewGenerator::getQuery), // 2
        INSERT(TiDBInsertGenerator::getQuery), // 3
        ALTER_TABLE(TiDBAlterTableGenerator::getQuery), // 4
        TRUNCATE((g) -> new SQLQueryAdapter("TRUNCATE " + g.getSchema().getRandomTable(t -> !t.isView()).getName())), // 5
        UPDATE(TiDBUpdateGenerator::getQuery), // 6
        DELETE(TiDBDeleteGenerator::getQuery), // 7
        SET(TiDBSetGenerator::getQuery), // 8
        ADMIN_CHECKSUM_TABLE(
                (g) -> new SQLQueryAdapter("ADMIN CHECKSUM TABLE " + g.getSchema().getRandomTable().getName())), // 9
        ANALYZE_TABLE(TiDBAnalyzeTableGenerator::getQuery), // 10
        DROP_TABLE(TiDBDropTableGenerator::dropTable), // 11
        DROP_VIEW(TiDBDropViewGenerator::dropView); // 12

        private final SQLQueryProvider<TiDBGlobalState> sqlQueryProvider;

        Action(SQLQueryProvider<TiDBGlobalState> sqlQueryProvider) {
            this.sqlQueryProvider = sqlQueryProvider;
        }

        @Override
        public SQLQueryAdapter getQuery(TiDBGlobalState state) throws Exception {
            return sqlQueryProvider.getQuery(state);
        }
    }

    public static class TiDBGlobalState extends SQLGlobalState<TiDBOptions, TiDBSchema> {

        @Override
        protected TiDBSchema readSchema() throws SQLException {
            return TiDBSchema.fromConnection(getConnection(), getDatabaseName());
        }

    }

    private static int mapActions(TiDBGlobalState globalState, Action a) {
        Randomly r = globalState.getRandomly();
        switch (a) {
        case ANALYZE_TABLE:
        case CREATE_INDEX:
            return r.getInteger(0, 2);
        case INSERT:
        case TRUNCATE:
        case DELETE:
        case ADMIN_CHECKSUM_TABLE:
            return r.getInteger(0, 2);
        case SET:
        case UPDATE:
            return r.getInteger(0, 5);
        case VIEW_GENERATOR:
            // https://github.com/tidb-challenge-program/bug-hunting-issue/issues/8
            return r.getInteger(0, 2);
        case ALTER_TABLE:
            return r.getInteger(0, 10); // https://github.com/tidb-challenge-program/bug-hunting-issue/issues/10
        case CREATE_TABLE:
        case DROP_TABLE:
        case DROP_VIEW:
            return 0;
        default:
            throw new AssertionError(a);
        }

    }

    @Override
    public void generateDatabase(TiDBGlobalState globalState) throws Exception {
        for (int i = 0; i < Randomly.fromOptions(1, 2); i++) {
            boolean success;
            do {
                SQLQueryAdapter qt = new TiDBTableGenerator().getQuery(globalState);
                success = globalState.executeStatement(qt);
            } while (!success);
        }

        StatementExecutor<TiDBGlobalState, Action> se = new StatementExecutor<>(globalState, Action.values(),
                TiDBProvider::mapActions, (q) -> {
                    if (globalState.getSchema().getDatabaseTables().isEmpty()) {
                        throw new IgnoreMeException();
                    }
                });
        try {
            se.executeStatements();
        } catch (SQLException e) {
            if (e.getMessage().contains(
                    "references invalid table(s) or column(s) or function(s) or definer/invoker of view lack rights to use them")) {
                throw new IgnoreMeException(); // TODO: drop view instead
            } else {
                throw new AssertionError(e);
            }
        }
    }

    @Override
    public SQLConnection createDatabase(TiDBGlobalState globalState) throws SQLException {
        String host = globalState.getOptions().getHost();
        int port = globalState.getOptions().getPort();
        if (host == null) {
            host = TiDBOptions.DEFAULT_HOST;
        }
        if (port == MainOptions.NO_SET_PORT) {
            port = TiDBOptions.DEFAULT_PORT;
        }

        String databaseName = globalState.getDatabaseName();
        String url = String.format("jdbc:mysql://%s:%d/", host, port);
        Connection con = DriverManager.getConnection(url, globalState.getOptions().getUserName(),
                globalState.getOptions().getPassword());
        globalState.getState().logStatement("USE test");
        globalState.getState().logStatement("DROP DATABASE IF EXISTS " + databaseName);
        String createDatabaseCommand = "CREATE DATABASE " + databaseName;
        globalState.getState().logStatement(createDatabaseCommand);
        globalState.getState().logStatement("USE " + databaseName);
        try (Statement s = con.createStatement()) {
            s.execute("DROP DATABASE IF EXISTS " + databaseName);
        }
        try (Statement s = con.createStatement()) {
            s.execute(createDatabaseCommand);
        }
        con.close();
        con = DriverManager.getConnection(url + databaseName, globalState.getOptions().getUserName(),
                globalState.getOptions().getPassword());
        return new SQLConnection(con);
    }

    @Override
    public String getDBMSName() {
        return "tidb";
    }

    @Override
    public synchronized boolean mutateTables(TiDBGlobalState globalState) throws Exception {
        // Update the post rewards
        if (currentMutationOperator != -1) {
            double k = globalState.getOptions().getQPGk();
            if (k == 0) {
                weightedAverageReward[currentMutationOperator] += ((double) currentSelectRewards
                        / (double) currentSelectCounts) / cumulativeMutationTimes[currentMutationOperator];
            } else {
                weightedAverageReward[currentMutationOperator] += ((double) currentSelectRewards
                        / (double) currentSelectCounts) / k; // Update the post rewards without updating the cumulative
                                                             // mutation times
            }
        }
        currentMutationOperator = -1;

        // Mutate tables
        int selectedActionIndex = 0;
        if (globalState.getOptions().enableRandomQPG()
                || Randomly.getPercentage() < globalState.getOptions().getQPGProbability()) {
            selectedActionIndex = globalState.getRandomly().getInteger(0, Action.values().length);
        } else {
            selectedActionIndex = getMaxRewardIndex();
        }
        cumulativeMutationTimes[selectedActionIndex]++;
        int reward = 0;
        // Debug
        // System.out.println(selectedActionIndex);

        try {
            SQLQueryAdapter queryMutateTable = Action.values()[selectedActionIndex].getQuery(globalState);
            globalState.executeStatement(queryMutateTable);

            // Remove the invalid views
            checkViewsAreValid(globalState);
            reward = checkQueryPlan(globalState);
        } catch (IgnoreMeException e) {
        } catch (AssertionError e) {
        } finally {
            updateReward(selectedActionIndex, (double) reward / (double) queryPlanPool.size(), globalState);
            currentMutationOperator = selectedActionIndex;
        }
        currentSelectRewards = 0;
        currentSelectCounts = 0;
        // System.out.println(Thread.currentThread().getName() + ": " + selectedActionIndex);
        // for (double weight : weightedAverageReward) {
        // System.out.print(weight + " ");
        // }
        // System.out.println("Weighted Average Reward");
        return true;
    }

    @Override
    public boolean addQueryPlan(TiDBGlobalState globalState, String selectStr) throws Exception {
        String queryPlan = getQueryPlan(globalState, selectStr);

        if (globalState.getOptions().logQueryPlan()) {
            globalState.getLogger().writeQueryPlan(queryPlan);
        }

        currentSelectCounts += 1;
        if (queryPlanPool.containsKey(queryPlan)) {
            return false;
        } else {
            queryPlanPool.put(queryPlan, selectStr);
            currentSelectRewards += 1;
            return true;
        }
    }

    private int checkQueryPlan(TiDBGlobalState globalState) throws Exception {
        int newQueryPlanFound = 0;
        HashMap<String, String> modifiedQueryPlan = new HashMap<>();
        for (Iterator<Map.Entry<String, String>> it = queryPlanPool.entrySet().iterator(); it.hasNext();) {
            Map.Entry<String, String> item = it.next();
            String queryPlan = item.getKey();
            String selectStr = item.getValue();
            String newQueryPlan = getQueryPlan(globalState, selectStr);
            if (newQueryPlan.isEmpty()) { // Invalid query
                it.remove();
            } else if (!queryPlan.equals(newQueryPlan)) { // The query plan has changed
                it.remove();
                modifiedQueryPlan.put(newQueryPlan, selectStr);
                if (!queryPlanPool.containsKey(newQueryPlan)) { // The new query plan found
                    newQueryPlanFound++;
                }
            }
        }
        queryPlanPool.putAll(modifiedQueryPlan);
        return newQueryPlanFound;
    }

    private int getMaxRewardIndex() {
        int maxIndex = 0;
        double maxReward = 0.0;

        for (int j = 0; j < weightedAverageReward.length; j++) {
            double curReward = weightedAverageReward[j];
            if (curReward > maxReward) {
                maxIndex = j;
                maxReward = curReward;
            }
        }

        return maxIndex;
    }

    private void updateReward(int actionIndex, double reward, TiDBGlobalState globalState) {
        double k = globalState.getOptions().getQPGk();
        if (k == 0) {
            weightedAverageReward[actionIndex] += (reward - weightedAverageReward[actionIndex])
                    / cumulativeMutationTimes[actionIndex];
        } else {
            weightedAverageReward[actionIndex] += (reward - weightedAverageReward[actionIndex]) / k; // The latest
                                                                                                     // reward always
                                                                                                     // takes k percent
                                                                                                     // of the total
                                                                                                     // reward
        }
    }

    @Override
    public String getQueryPlan(TiDBGlobalState globalState, String selectStr) throws Exception {
        String queryPlan = "";
        if (globalState.getOptions().logEachSelect()) {
            globalState.getLogger().writeCurrent(selectStr);
            try {
                globalState.getLogger().getCurrentFileWriter().flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        SQLQueryAdapter q = new SQLQueryAdapter("EXPLAIN " + selectStr, null);
        try (SQLancerResultSet rs = q.executeAndGet(globalState)) {
            if (rs != null) {
                while (rs.next()) {
                    String targetQueryPlan = rs.getString(1).replace("├─", "").replace("└─", "").replace("│", "").trim()
                            + ";"; // Unify format
                    queryPlan += targetQueryPlan;
                }
            }
        } catch (Throwable e) {
            e.printStackTrace();
        }

        return queryPlan;
    }
}
