package sqlancer.cockroachdb;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

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

    public CockroachDBProvider() {
        super(CockroachDBGlobalState.class, CockroachDBOptions.class);
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
                new ExpectedErrors())),

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
        standardSettings.add("--Don't send automatic bug reports");
        standardSettings.add("SET CLUSTER SETTING debug.panic_on_failed_assertions = true;");
        standardSettings.add("SET CLUSTER SETTING diagnostics.reporting.enabled    = false;");
        standardSettings.add("SET CLUSTER SETTING diagnostics.reporting.send_crash_reports = false;");

        standardSettings.add("-- Disable the collection of metrics and hope that it helps performance");
        standardSettings.add("SET CLUSTER SETTING sql.metrics.statement_details.enabled = 'off'");
        standardSettings.add("SET CLUSTER SETTING sql.metrics.statement_details.plan_collection.enabled = 'off'");
        standardSettings.add("SET CLUSTER SETTING sql.stats.automatic_collection.enabled = 'off'");
        // N.B. disabling timeseries.storage effectively means no metrics in the DB Console, so let's keep them.
        standardSettings.add("SET CLUSTER SETTING timeseries.storage.enabled = 'on'");

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
                nrPerformed = globalState.getRandomly().getInteger(0, 5);
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
                nrPerformed = globalState.getRandomly().getInteger(0, 2);
                break;
            case SCRUB:
                nrPerformed = globalState.getRandomly().getInteger(0, 7);
                break;
            case TRANSACTION:
                nrPerformed = globalState.getRandomly().getInteger(0, 3);
                break;
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

        if (globalState.getDbmsSpecificOptions().getTestOracleFactory().stream()
                .anyMatch((o) -> o == CockroachDBOracleFactory.CERT)) {
            // Enfore statistic collected for all tables
            ExpectedErrors errors = new ExpectedErrors();
            CockroachDBErrors.addExpressionErrors(errors);
            for (CockroachDBTable table : globalState.getSchema().getDatabaseTables()) {
                globalState.executeStatement(new SQLQueryAdapter("ANALYZE " + table.getName() + ";", errors));
            }
        }
    }

    // Constructs a full pgurl of the following forms,
    //
    // 1. jdbc:postgresql://$host:$port/$db
    // 2. jdbc:postgresql://$host_1:$port,$host_2:$port,...,$host_n:$port/$db?loadBalanceHosts=true
    // 3. jdbc:postgresql://$host:$port/$db?query_options
    //
    // depending on the 'host' specification. If the host contains '?', then form 3 is used,
    // by merely replacing encoded database name with the specified one. If the host contains ',', then form 2 is used.
    // Otherwise, form 1 is used.
    String buildPGUrl(String host, int port, String databaseName) throws SQLException {
        String url = String.format("jdbc:postgresql://%s:%d/%s", host, port, databaseName);
        if (host.contains("?")) {
            // assume host encodes a complete pgurl; attempt to replace the database name between '/' and '?'
            int endIndex = host.indexOf('?');
            int startIndex = host.lastIndexOf('/');
            if (startIndex != -1 && startIndex < endIndex) {
                host = host.substring(0, startIndex) + "/" + databaseName + "?" + host.substring(endIndex+1);
                url = String.format("jdbc:postgresql://%s", host);
            } else {
                throw new SQLException("'host' encodes an invalid pgurl: " + host);
            }
        } else if (host.indexOf(',') != -1) {
            String[] hosts = host.split(",");
            StringBuilder hostAndPort = new StringBuilder();
            String sep = "";
            for (String h : hosts) {
                hostAndPort.append(sep);
                hostAndPort.append(h);
                hostAndPort.append(":");
                hostAndPort.append(port);
                sep = ",";
            }
            url = String.format("jdbc:postgresql://%s/%s?loadBalanceHosts=true", hostAndPort, databaseName);
        }
        return url;
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
        String url = buildPGUrl(host, port, databaseName);
        System.out.println("JDBC url="+ url);

        Connection con = DriverManager.getConnection(url, globalState.getOptions().getUserName(),
                globalState.getOptions().getPassword());
        
        // N.B. 'defaultdb' is always there and never goes away, use it to drop/create other databases.
        globalState.getState().logStatement("USE defaultdb");
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
        con = DriverManager.getConnection(url,
                globalState.getOptions().getUserName(), globalState.getOptions().getPassword());
        return new SQLConnection(con);
    }

    @Override
    public String getDBMSName() {
        return "cockroachdb";
    }

    @Override
    public String getQueryPlan(String selectStr, CockroachDBGlobalState globalState) throws Exception {
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
        SQLQueryAdapter q = new SQLQueryAdapter(explainQuery);
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
            throw new AssertionError("Explain failed: " + explainQuery, e);
        }

        return queryPlan;
    }

    @Override
    protected double[] initializeWeightedAverageReward() {
        return new double[Action.values().length];
    }

    @Override
    protected void executeMutator(int index, CockroachDBGlobalState globalState) throws Exception {
        SQLQueryAdapter queryMutateTable = Action.values()[index].getQuery(globalState);
        globalState.executeStatement(queryMutateTable);
    }

    @Override
    public boolean addRowsToAllTables(CockroachDBGlobalState globalState) throws Exception {
        List<CockroachDBTable> tablesNoRow = globalState.getSchema().getDatabaseTables().stream()
                .filter(t -> t.getNrRows(globalState) == 0).collect(Collectors.toList());
        for (CockroachDBTable table : tablesNoRow) {
            SQLQueryAdapter queryAddRows = CockroachDBInsertGenerator.insert(globalState, table);
            globalState.executeStatement(queryAddRows);
        }
        return true;
    }

}
