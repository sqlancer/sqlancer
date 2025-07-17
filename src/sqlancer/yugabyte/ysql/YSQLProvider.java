package sqlancer.yugabyte.ysql;

import com.google.auto.service.AutoService;
import sqlancer.*;
import sqlancer.common.DBMSCommon;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLQueryProvider;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.yugabyte.ysql.gen.*;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

import static sqlancer.yugabyte.ysql.YSQLOptions.YSQLOracleFactory.CATALOG;

@AutoService(DatabaseProvider.class)
public class YSQLProvider extends SQLProviderAdapter<YSQLGlobalState, YSQLOptions> {

    /**
     * Generate only data types and expressions that are understood by PQS.
     */
    public static boolean generateOnlyKnown;
    protected String entryURL;
    protected String username;
    protected String password;
    protected String entryPath;
    protected String host;
    protected int port;
    protected String testURL;
    protected String databaseName;
    protected String createDatabaseCommand;

    public YSQLProvider() {
        super(YSQLGlobalState.class, YSQLOptions.class);
    }

    protected YSQLProvider(Class<YSQLGlobalState> globalClass, Class<YSQLOptions> optionClass) {
        super(globalClass, optionClass);
    }

    public static int mapActions(YSQLGlobalState globalState, Action a) {
        Randomly r = globalState.getRandomly();
        int nrPerformed;
        switch (a) {
            case CREATE_INDEX:
                nrPerformed = r.getInteger(0, 3);
                break;
            case DISCARD:
            case DROP_INDEX:
                nrPerformed = r.getInteger(0, 5);
                break;
            case COMMIT:
                nrPerformed = r.getInteger(0, 3);
                break;
            case SET_TRANSACTION:
                nrPerformed = r.getInteger(0, 2);
                break;
            case PARALLEL_QUERY_TEST:
                nrPerformed = r.getInteger(0, 1);
                break;
            case ALTER_TABLE:
                nrPerformed = r.getInteger(0, 5);
                break;
            case RESET:
                nrPerformed = r.getInteger(0, 3);
                break;
            case ANALYZE:
                nrPerformed = r.getInteger(0, 3);
                break;
            case DELETE:
            case RESET_ROLE:
            case VACUUM:
            case SET_CONSTRAINTS:
            case SET:
            case COMMENT_ON:
//            case NOTIFY:
//            case LISTEN:
//            case UNLISTEN:
            case CREATE_SEQUENCE:
            case TRUNCATE:
                nrPerformed = r.getInteger(0, 15);
                break;
            case CREATE_VIEW:
                nrPerformed = r.getInteger(0, 5);
                break;
            case REFRESH_VIEW:
                nrPerformed = r.getInteger(0, 20);
                break;
            case UPDATE:
                nrPerformed = r.getInteger(0, 20);
                break;
            case INSERT:
                nrPerformed = r.getInteger(0, globalState.getOptions().getMaxNumberInserts());
                break;
            default:
                throw new AssertionError(a);
        }
        return nrPerformed;

    }

    @Override
    public void generateDatabase(YSQLGlobalState globalState) throws Exception {
        if (globalState.getDbmsSpecificOptions().createDatabases) {
            readFunctions(globalState);
            createTables(globalState, Randomly.fromOptions(4, 5, 6));
            prepareTables(globalState);
        }
    }

    @Override
    public SQLConnection createDatabase(YSQLGlobalState globalState) throws SQLException {
        username = globalState.getOptions().getUserName();
        password = globalState.getOptions().getPassword();
        host = globalState.getOptions().getHost();
        port = globalState.getOptions().getPort();
        entryPath = "/yugabyte";
        entryURL = globalState.getDbmsSpecificOptions().connectionURL;
        String entryDatabaseName = entryPath.substring(1);
        databaseName = globalState.getDatabaseName();

        if (host == null) {
            host = YSQLOptions.DEFAULT_HOST;
        }
        if (port == MainOptions.NO_SET_PORT) {
            port = YSQLOptions.DEFAULT_PORT;
        }

        try {
            URI uri = new URI(entryURL);
            String userInfoURI = uri.getUserInfo();
            String pathURI = uri.getPath();
            if (userInfoURI != null) {
                // username and password specified in URL take precedence
                if (userInfoURI.contains(":")) {
                    String[] userInfo = userInfoURI.split(":", 2);
                    username = userInfo[0];
                    password = userInfo[1];
                } else {
                    username = userInfoURI;
                    password = null;
                }
                int userInfoIndex = entryURL.indexOf(userInfoURI);
                String preUserInfo = entryURL.substring(0, userInfoIndex);
                String postUserInfo = entryURL.substring(userInfoIndex + userInfoURI.length() + 1);
                entryURL = preUserInfo + postUserInfo;
            }
            if (pathURI != null) {
                entryPath = pathURI;
            }
            if (host == null) {
                host = uri.getHost();
            }
            if (port == MainOptions.NO_SET_PORT) {
                port = uri.getPort();
            }
            entryURL = String.format("jdbc:yugabytedb://%s:%d/%s", host, port, entryDatabaseName);
        } catch (URISyntaxException e) {
            throw new AssertionError(e);
        }

        if (globalState.getDbmsSpecificOptions().createDatabases)
            createDatabaseSync(globalState, entryDatabaseName);

        int databaseIndex = entryURL.indexOf("/" + entryDatabaseName) + 1;
        String preDatabaseName = entryURL.substring(0, databaseIndex);
        String postDatabaseName = entryURL.substring(databaseIndex + entryDatabaseName.length());
        testURL = preDatabaseName + databaseName + postDatabaseName;
        globalState.getState().logStatement(String.format("\\c %s;", databaseName));

        return new SQLConnection(createConnectionSafely(testURL, username, password));
    }

    @Override
    public String getDBMSName() {
        return "ysql";
    }

    // for some reason yugabyte unable to create few databases simultaneously
    private void createDatabaseSync(YSQLGlobalState globalState, String entryDatabaseName) throws SQLException {
        int counter = 0;
        while (true) {
            try (Connection con = createConnectionSafely(entryURL, username, password)) {
                globalState.getState().logStatement(String.format("\\c %s;", entryDatabaseName));
                globalState.getState().logStatement("DROP DATABASE IF EXISTS " + databaseName);
                createDatabaseCommand = getCreateDatabaseCommand(globalState);
                globalState.getState().logStatement(createDatabaseCommand);
                try (Statement s = con.createStatement()) {
                    s.execute("DROP DATABASE IF EXISTS " + databaseName);
                }
                try (Statement s = con.createStatement()) {
                    s.execute(createDatabaseCommand);
                }
                break;
            } catch (Exception e) {
                if ((e.getMessage().contains("Catalog Version Mismatch") || e.getMessage().contains("Restart read required")
                        || e.getMessage().contains("could not serialize access due to concurrent update")
                        || e.getMessage().contains("not onlined")
                        || e.getMessage().contains("is being accessed by other users")
                        || e.getMessage().contains("Restarting a DDL transaction not supported"))
                        && counter < 20) {
                    counter++;
                    exceptionLessSleep(500);
                } else {
                    throw e;
                }
            }
        }
    }

    private Connection createConnectionSafely(String entryURL, String user, String password) {
        Connection con = null;
        IllegalStateException lastException = new IllegalStateException("Empty exception");
        long endTime = System.currentTimeMillis() + 30000;
        while (System.currentTimeMillis() < endTime) {
            try {
                con = DriverManager.getConnection(entryURL, user, password);
                break;
            } catch (SQLException throwables) {
                lastException = new IllegalStateException(throwables);
            }
        }

        if (con == null) {
            throw lastException;
        }

        return con;
    }

    protected void readFunctions(YSQLGlobalState globalState) throws SQLException {
        SQLQueryAdapter query = new SQLQueryAdapter("SELECT proname, provolatile FROM pg_proc;");
        SQLancerResultSet rs = query.executeAndGet(globalState);
        while (rs.next()) {
            String functionName = rs.getString(1);
            Character functionType = rs.getString(2).charAt(0);
            globalState.addFunctionAndType(functionName, functionType);
        }
    }

    protected void createTables(YSQLGlobalState globalState, int numTables) throws Exception {
        while (globalState.getSchema().getDatabaseTables().size() < numTables) {
            try {
                String tableName = DBMSCommon.createTableName(globalState.getSchema().getDatabaseTables().size());
                SQLQueryAdapter createTable = YSQLTableGenerator.generate(tableName, generateOnlyKnown,
                        globalState);
                globalState.executeStatement(createTable);
            } catch (IgnoreMeException e) {
                // do nothing
            }
        }
    }

    private void exceptionLessSleep(long timeout) {
        try {
            Thread.sleep(timeout);
        } catch (InterruptedException e) {
            throw new AssertionError();
        }
    }

    protected void prepareTables(YSQLGlobalState globalState) throws Exception {
        StatementExecutor<YSQLGlobalState, Action> se = new StatementExecutor<>(globalState, Action.values(),
                YSQLProvider::mapActions, (q) -> {
            if (globalState.getSchema().getDatabaseTables().isEmpty()) {
                throw new IgnoreMeException();
            }
        });
        se.executeStatements();
        globalState.executeStatement(new SQLQueryAdapter("COMMIT", true));
        globalState.executeStatement(new SQLQueryAdapter("SET SESSION statement_timeout = 15000;\n"));
    }

    private String getCreateDatabaseCommand(YSQLGlobalState state) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE DATABASE ").append(databaseName).append(" ");
        if (CATALOG.equals(state.getDbmsSpecificOptions().oracle.get(0))) {
            // Force colocation true if CATALOG test is selected
            sb.append("WITH ");
            if (Randomly.getPercentage() > 0.05) {
                sb.append("COLOCATION = true ");
            }

            if (Randomly.getBoolean() && state.getDbmsSpecificOptions().testCollations) {
                if (Randomly.getBoolean()) {
                    sb.append("ENCODING '");
                    sb.append(Randomly.fromOptions("utf8"));
                    sb.append("' ");
                }
                for (String lc : Arrays.asList("LC_COLLATE", "LC_CTYPE")) {
                    if (!state.getCollates().isEmpty() && Randomly.getBoolean()) {
                        sb.append(String.format(" %s = '%s'", lc, Randomly.fromList(state.getCollates())));
                    }
                }
                sb.append(" TEMPLATE template0");

            }
        } else {
            if (Randomly.getBoolean() && state.getDbmsSpecificOptions().testCollations) {
                sb.append("WITH ");
                if (Randomly.getBoolean()) {
                    sb.append("ENCODING '");
                    sb.append(Randomly.fromOptions("utf8"));
                    sb.append("' ");
                }

                // create non colocated database with low priority to avoid cluster resource issues
                if (Randomly.getPercentage() > 0.05) {
                    sb.append("COLOCATION = true ");
                }

                for (String lc : Arrays.asList("LC_COLLATE", "LC_CTYPE")) {
                    if (!state.getCollates().isEmpty() && Randomly.getBoolean()) {
                        sb.append(String.format(" %s = '%s'", lc, Randomly.fromList(state.getCollates())));
                    }
                }
                sb.append(" TEMPLATE template0");

            }
        }
        return sb.toString();
    }

    public enum Action implements AbstractAction<YSQLGlobalState> {
        ANALYZE(YSQLAnalyzeGenerator::create), //
        ALTER_TABLE(g -> YSQLAlterTableGenerator.create(g.getSchema().getRandomTable(t -> !t.isView()), g)), //
        COMMIT(g -> {
            SQLQueryAdapter query;
            if (Randomly.getBoolean()) {
                query = new SQLQueryAdapter("COMMIT", true);
            } else if (Randomly.getBoolean()) {
                query = YSQLTransactionGenerator.executeBegin();
            } else {
                query = new SQLQueryAdapter("ROLLBACK", true);
            }
            return query;
        }), //
        DELETE(YSQLDeleteGenerator::create), //
        DISCARD(YSQLDiscardGenerator::create), //
        DROP_INDEX(YSQLDropIndexGenerator::create), //
        CREATE_INDEX(YSQLIndexGenerator::generate), //
        INSERT(YSQLInsertGenerator::insert), //
        UPDATE(YSQLUpdateGenerator::create), //
        TRUNCATE(YSQLTruncateGenerator::create), //
        VACUUM(YSQLVacuumGenerator::create), //
        SET(YSQLSetGenerator::create), // TODO insert yugabyte sets
        SET_CONSTRAINTS((g) -> {
            String sb = "SET CONSTRAINTS ALL " + Randomly.fromOptions("DEFERRED", "IMMEDIATE");
            return new SQLQueryAdapter(sb);
        }), //
        SET_TRANSACTION(YSQLTransactionGenerator::setTransactionMode), //
        RESET_ROLE((g) -> new SQLQueryAdapter("RESET ROLE")), //
        COMMENT_ON(YSQLCommentGenerator::generate), //
        RESET((g) -> new SQLQueryAdapter("RESET ALL") /*
         * https://www.postgres.org/docs/devel/sql-reset.html TODO: also
         * configuration parameter
         */), //
        //        NOTIFY(YSQLNotifyGenerator::createNotify), //
//        LISTEN((g) -> YSQLNotifyGenerator.createListen()), //
//        UNLISTEN((g) -> YSQLNotifyGenerator.createUnlisten()), //
        CREATE_SEQUENCE(YSQLSequenceGenerator::createSequence), //
        CREATE_VIEW(YSQLViewGenerator::create),
        REFRESH_VIEW(YSQLMaterializedViewRefresh::create),
        PARALLEL_QUERY_TEST(YSQLParallelQueryGenerator::generateParallelQueryTest);

        private final SQLQueryProvider<YSQLGlobalState> sqlQueryProvider;

        Action(SQLQueryProvider<YSQLGlobalState> sqlQueryProvider) {
            this.sqlQueryProvider = sqlQueryProvider;
        }

        @Override
        public SQLQueryAdapter getQuery(YSQLGlobalState state) throws Exception {
            return sqlQueryProvider.getQuery(state);
        }
    }

}
