package sqlancer.cockroachdb;

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

@AutoService(DatabaseProvider.class)
public class CockroachDBProvider extends SQLProviderAdapter<CockroachDBGlobalState, CockroachDBOptions> {

    public CockroachDBProvider() {
        super(CockroachDBGlobalState.class, CockroachDBOptions.class);
    }

    public enum Action {
        INSERT(CockroachDBInsertGenerator::insert), //
        TRUNCATE(CockroachDBTruncateGenerator::truncate), //
        CREATE_STATISTICS(CockroachDBCreateStatisticsGenerator::create), //
        SET_SESSION(CockroachDBSetSessionGenerator::create), //
        CREATE_INDEX(CockroachDBIndexGenerator::create), //
        UPDATE(CockroachDBUpdateGenerator::gen), //
        CREATE_VIEW(CockroachDBViewGenerator::generate), //
        SET_CLUSTER_SETTING(CockroachDBSetClusterSettingGenerator::create), //
        DELETE(CockroachDBDeleteGenerator::delete), //
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
                sb.append(Randomly.nonEmptySubset("VERBOSE", "TYPES", "OPT", "DISTSQL", "VEC").stream()
                        .collect(Collectors.joining(", ")));
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
            manager.execute(new SQLQueryAdapter("SET vectorize=on;"));
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
        con = DriverManager.getConnection("jdbc:postgresql://localhost:26257/" + databaseName,
                globalState.getOptions().getUserName(), globalState.getOptions().getPassword());
        return new SQLConnection(con);
    }

    @Override
    public String getDBMSName() {
        return "cockroachdb";
    }

}
