package sqlancer.cockroachdb;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.auto.service.AutoService;

import sqlancer.DatabaseProvider;
import sqlancer.IgnoreMeException;
import sqlancer.Main.QueryManager;
import sqlancer.MainOptions;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.SQLGlobalState;
import sqlancer.SQLProviderAdapter;
import sqlancer.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBTable;
import sqlancer.cockroachdb.gen.CockroachDBCommentOnGenerator;
import sqlancer.cockroachdb.gen.CockroachDBCreateStatisticsGenerator;
import sqlancer.cockroachdb.gen.CockroachDBDeleteGenerator;
import sqlancer.cockroachdb.gen.CockroachDBDropTableGenerator;
import sqlancer.cockroachdb.gen.CockroachDBDropViewGenerator;
import sqlancer.cockroachdb.gen.CockroachDBIndexGenerator;
import sqlancer.cockroachdb.gen.CockroachDBInsertGenerator;
import sqlancer.cockroachdb.gen.CockroachDBRandomQuerySynthesizer;
import sqlancer.cockroachdb.gen.CockroachDBSetClusterSettingGenerator;
import sqlancer.cockroachdb.gen.CockroachDBSetSessionGenerator;
import sqlancer.cockroachdb.gen.CockroachDBShowGenerator;
import sqlancer.cockroachdb.gen.CockroachDBTableGenerator;
import sqlancer.cockroachdb.gen.CockroachDBTruncateGenerator;
import sqlancer.cockroachdb.gen.CockroachDBUpdateGenerator;
import sqlancer.cockroachdb.gen.CockroachDBViewGenerator;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLQueryProvider;
import sqlancer.common.query.SQLancerResultSet;

@AutoService(DatabaseProvider.class)
public class CockroachDBProvider extends SQLProviderAdapter<CockroachDBGlobalState, CockroachDBOptions> {
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

    public CockroachDBProvider() {
        super(CockroachDBGlobalState.class, CockroachDBOptions.class);
        queryPlanPool = new HashMap<>();
        weightedAverageReward = new double[Action.values().length];
        cumulativeMutationTimes = new int[Action.values().length];

        // For post reward calculation
        currentSelectRewards = 0;
        currentSelectCounts = 0;
        currentMutationOperator = -1;
    }

    public enum Action {
        CREATE_TABLE(CockroachDBTableGenerator::generate), CREATE_INDEX(CockroachDBIndexGenerator::create), //
        CREATE_VIEW(CockroachDBViewGenerator::generate), //
        CREATE_STATISTICS(CockroachDBCreateStatisticsGenerator::create), //
        INSERT(CockroachDBInsertGenerator::insert), //
        UPDATE(CockroachDBUpdateGenerator::gen), //
        SET_SESSION(CockroachDBSetSessionGenerator::create), //
        SET_CLUSTER_SETTING(CockroachDBSetClusterSettingGenerator::create), //
        DELETE(CockroachDBDeleteGenerator::delete), //
        TRUNCATE(CockroachDBTruncateGenerator::truncate), //
        DROP_TABLE(CockroachDBDropTableGenerator::drop), //
        DROP_VIEW(CockroachDBDropViewGenerator::drop), //
        COMMENT_ON(CockroachDBCommentOnGenerator::comment), //
        SHOW(CockroachDBShowGenerator::show), //
        TRANSACTION((g) -> {
            String s = Randomly.fromOptions("BEGIN", "ROLLBACK", "COMMIT");
            return new SQLQueryAdapter(s, ExpectedErrors.from("there is no transaction in progress",
                    "there is already a transaction in progress", "current transaction is aborted"));
        }), EXPLAIN((g) -> {
            StringBuilder sb = new StringBuilder("EXPLAIN ");
            ExpectedErrors errors = new ExpectedErrors();
            if (Randomly.getBoolean()) {
                sb.append("(");
                sb.append(Randomly.fromOptions("VERBOSE", "TYPES", "OPT", "DISTSQL", "VEC"));
                sb.append(") ");
                errors.add("cannot set EXPLAIN mode more than once");
                errors.add("unable to vectorize execution plan");
                errors.add("unsupported type");
                errors.add("vectorize is set to 'off'");
            }
            sb.append(CockroachDBRandomQuerySynthesizer.generate(g, Randomly.smallNumber() + 1));
            CockroachDBErrors.addExpressionErrors(errors);
            return new SQLQueryAdapter(sb.toString(), errors);
        }), //
        SCRUB((g) -> new SQLQueryAdapter(
                "EXPERIMENTAL SCRUB table " + g.getSchema().getRandomTable(t -> !t.isView()).getName(),
                // https://github.com/cockroachdb/cockroach/issues/46401
                ExpectedErrors.from("scrub-fk: column \"t.rowid\" does not exist",
                        "check-constraint: cannot access temporary tables of other sessions" /*
                                                                                              * https:// github. com/
                                                                                              * cockroachdb / cockroach
                                                                                              * /issues/ 47031
                                                                                              */))), //
        SPLIT((g) -> {
            StringBuilder sb = new StringBuilder("ALTER INDEX ");
            CockroachDBTable randomTable = g.getSchema().getRandomTable();
            sb.append(randomTable.getName());
            sb.append("@");
            sb.append(randomTable.getRandomIndex());
            if (Randomly.getBoolean()) {
                sb.append(" SPLIT AT VALUES (true), (false);");
            } else {
                sb.append(" SPLIT AT VALUES (NULL);");
            }
            return new SQLQueryAdapter(sb.toString(), ExpectedErrors.from("must be of type"));
        });

        private final SQLQueryProvider<CockroachDBGlobalState> sqlQueryProvider;

        Action(SQLQueryProvider<CockroachDBGlobalState> sqlQueryProvider) {
            this.sqlQueryProvider = sqlQueryProvider;
        }

        public SQLQueryAdapter getQuery(CockroachDBGlobalState state) throws Exception {
            return sqlQueryProvider.getQuery(state);
        }
    }

    public static class CockroachDBGlobalState extends SQLGlobalState<CockroachDBOptions, CockroachDBSchema> {

        @Override
        protected CockroachDBSchema readSchema() throws SQLException {
            return CockroachDBSchema.fromConnection(getConnection(), getDatabaseName());
        }

    }

    @Override
    public void generateDatabase(CockroachDBGlobalState globalState) throws Exception {
        QueryManager<SQLConnection> manager = globalState.getManager();
        MainOptions options = globalState.getOptions();
        List<String> standardSettings = new ArrayList<>();
        standardSettings.add("--Don't send automatic bug reports\n"
                + "SET CLUSTER SETTING debug.panic_on_failed_assertions = true;");
        standardSettings.add("SET CLUSTER SETTING diagnostics.reporting.enabled    = false;");
        standardSettings.add("SET CLUSTER SETTING diagnostics.reporting.send_crash_reports = false;");

        standardSettings.add("-- Disable the collection of metrics and hope that it helps performance\n"
                + "SET CLUSTER SETTING sql.metrics.statement_details.enabled = 'off'");
        standardSettings.add("SET CLUSTER SETTING sql.metrics.statement_details.plan_collection.enabled = 'off'");
        standardSettings.add("SET CLUSTER SETTING sql.stats.automatic_collection.enabled = 'off'");
        standardSettings.add("SET CLUSTER SETTING timeseries.storage.enabled = 'off'");

        if (globalState.getDbmsSpecificOptions().testHashIndexes) {
            standardSettings.add("set experimental_enable_hash_sharded_indexes='on';");
        }
        if (globalState.getDbmsSpecificOptions().testTempTables) {
            standardSettings.add("SET experimental_enable_temp_tables = 'on'");
        }
        for (String s : standardSettings) {
            manager.execute(new SQLQueryAdapter(s));
        }

        for (int i = 0; i < Randomly.fromOptions(2, 3); i++) {
            boolean success = false;
            do {
                try {
                    SQLQueryAdapter q = CockroachDBTableGenerator.generate(globalState);
                    success = globalState.executeStatement(q);
                } catch (IgnoreMeException e) {
                    // continue trying
                }
            } while (!success);
        }

        int[] nrRemaining = new int[Action.values().length];
        List<Action> actions = new ArrayList<>();
        int total = 0;
        for (int i = 0; i < Action.values().length; i++) {
            Action action = Action.values()[i];
            int nrPerformed = 0;
            switch (action) {
            case INSERT:
                nrPerformed = globalState.getRandomly().getInteger(0, options.getMaxNumberInserts());
                break;
            case UPDATE:
            case SPLIT:
                nrPerformed = globalState.getRandomly().getInteger(0, 3);
                break;
            case EXPLAIN:
                nrPerformed = globalState.getRandomly().getInteger(0, 10);
                break;
            case SHOW:
            case TRUNCATE:
            case DELETE:
            case CREATE_STATISTICS:
                nrPerformed = globalState.getRandomly().getInteger(0, 2);
                break;
            case CREATE_VIEW:
                nrPerformed = globalState.getRandomly().getInteger(0, 2);
                break;
            case SET_SESSION:
            case SET_CLUSTER_SETTING:
                nrPerformed = globalState.getRandomly().getInteger(0, 3);
                break;
            case CREATE_INDEX:
                nrPerformed = globalState.getRandomly().getInteger(0, 10);
                break;
            case COMMENT_ON:
            case SCRUB:
                nrPerformed = 0; /*
                                  * there are a number of open SCRUB bugs, of which
                                  * https://github.com/cockroachdb/cockroach/issues/47116 crashes the server
                                  */
                break;
            case TRANSACTION:
            case CREATE_TABLE:
            case DROP_TABLE:
            case DROP_VIEW:
                nrPerformed = 0; // r.getInteger(0, 0);
                break;
            default:
                throw new AssertionError(action);
            }
            if (nrPerformed != 0) {
                actions.add(action);
            }
            nrRemaining[action.ordinal()] = nrPerformed;
            total += nrPerformed;
        }

        while (total != 0) {
            Action nextAction = null;
            int selection = globalState.getRandomly().getInteger(0, total);
            int previousRange = 0;
            for (int i = 0; i < nrRemaining.length; i++) {
                if (previousRange <= selection && selection < previousRange + nrRemaining[i]) {
                    nextAction = Action.values()[i];
                    break;
                } else {
                    previousRange += nrRemaining[i];
                }
            }
            assert nextAction != null;
            assert nrRemaining[nextAction.ordinal()] > 0;
            nrRemaining[nextAction.ordinal()]--;
            SQLQueryAdapter query = null;
            try {
                boolean success;
                int nrTries = 0;
                do {
                    query = nextAction.getQuery(globalState);
                    success = globalState.executeStatement(query);
                } while (!success && nrTries++ < 1000);
            } catch (IgnoreMeException e) {

            }
            if (query != null && query.couldAffectSchema() && globalState.getSchema().getDatabaseTables().isEmpty()) {
                throw new IgnoreMeException();
            }
            total--;
        }
        if (globalState.getDbmsSpecificOptions().makeVectorizationMoreLikely && Randomly.getBoolean()) {
            manager.execute(new SQLQueryAdapter("SET vectorize=off;"));
        }
    }

    @Override
    public SQLConnection createDatabase(CockroachDBGlobalState globalState) throws SQLException {
        String host = globalState.getOptions().getHost();
        int port = globalState.getOptions().getPort();
        if (host == null) {
            host = CockroachDBOptions.DEFAULT_HOST;
        }
        if (port == MainOptions.NO_SET_PORT) {
            port = CockroachDBOptions.DEFAULT_PORT;
        }
        String databaseName = globalState.getDatabaseName();
        String url = String.format("jdbc:postgresql://%s:%d/test", host, port);
        Connection con = DriverManager.getConnection(url, globalState.getOptions().getUserName(),
                globalState.getOptions().getPassword());
        globalState.getState().logStatement("USE test");
        globalState.getState().logStatement("DROP DATABASE IF EXISTS " + databaseName + " CASCADE");
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
        con = DriverManager.getConnection(String.format("jdbc:postgresql://%s:%d/%s", host, port, databaseName),
                globalState.getOptions().getUserName(), globalState.getOptions().getPassword());
        return new SQLConnection(con);
    }

    @Override
    public String getDBMSName() {
        return "cockroachdb";
    }

    @Override
    public synchronized boolean mutateTables(CockroachDBGlobalState globalState) throws Exception {
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
        // Debug
        // System.out.println(Thread.currentThread().getName() + ": " + selectedActionIndex);
        // for (double weight : weightedAverageReward) {
        // System.out.print(weight + " ");
        // }
        // System.out.println("Weighted Average Reward");
        return true;
    }

    @Override
    public boolean addQueryPlan(CockroachDBGlobalState globalState, String selectStr) throws Exception {
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

    private int checkQueryPlan(CockroachDBGlobalState globalState) throws Exception {
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

    private void updateReward(int actionIndex, double reward, CockroachDBGlobalState globalState) {
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
    public String getQueryPlan(CockroachDBGlobalState globalState, String selectStr) throws Exception {
        String queryPlan = "";
        String explainQuery = "EXPLAIN (OPT) " + selectStr;
        if (globalState.getOptions().logEachSelect()) {
            globalState.getLogger().writeCurrent(explainQuery);
            try {
                globalState.getLogger().getCurrentFileWriter().flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        SQLQueryAdapter q = new SQLQueryAdapter(explainQuery, null);
        boolean afterProjection = false; // Remove the concrete expression after each Projection operator
        try (SQLancerResultSet rs = q.executeAndGet(globalState)) {
            if (rs != null) {
                while (rs.next()) {
                    String targetQueryPlan = rs.getString(1).replace("└──", "").replace("├──", "").replace("│", "")
                            .trim() + ";"; // Unify format
                    if (afterProjection) {
                        afterProjection = false;
                        continue;
                    }
                    if (targetQueryPlan.startsWith("projections")) {
                        afterProjection = true;
                    }
                    // Remove all concrete expressions by keywords
                    if (targetQueryPlan.contains(">") || targetQueryPlan.contains("<") || targetQueryPlan.contains("=")
                            || targetQueryPlan.contains("*") || targetQueryPlan.contains("+")
                            || targetQueryPlan.contains("'")) {
                        continue;
                    }
                    queryPlan += targetQueryPlan;
                }
            }
        } catch (AssertionError e) {
            new AssertionError("Explain failed: " + explainQuery);
        }

        return queryPlan;
    }

}
