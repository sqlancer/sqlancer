package sqlancer.yugabyte.ysql;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

import com.google.auto.service.AutoService;

import sqlancer.AbstractAction;
import sqlancer.DatabaseProvider;
import sqlancer.IgnoreMeException;
import sqlancer.MainOptions;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.SQLProviderAdapter;
import sqlancer.StatementExecutor;
import sqlancer.common.DBMSCommon;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLQueryProvider;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.yugabyte.ysql.gen.YSQLAlterTableGenerator;
import sqlancer.yugabyte.ysql.gen.YSQLAnalyzeGenerator;
import sqlancer.yugabyte.ysql.gen.YSQLCommentGenerator;
import sqlancer.yugabyte.ysql.gen.YSQLDeleteGenerator;
import sqlancer.yugabyte.ysql.gen.YSQLDiscardGenerator;
import sqlancer.yugabyte.ysql.gen.YSQLDropIndexGenerator;
import sqlancer.yugabyte.ysql.gen.YSQLIndexGenerator;
import sqlancer.yugabyte.ysql.gen.YSQLInsertGenerator;
import sqlancer.yugabyte.ysql.gen.YSQLNotifyGenerator;
import sqlancer.yugabyte.ysql.gen.YSQLSequenceGenerator;
import sqlancer.yugabyte.ysql.gen.YSQLSetGenerator;
import sqlancer.yugabyte.ysql.gen.YSQLTableGenerator;
import sqlancer.yugabyte.ysql.gen.YSQLTableGroupGenerator;
import sqlancer.yugabyte.ysql.gen.YSQLTransactionGenerator;
import sqlancer.yugabyte.ysql.gen.YSQLTruncateGenerator;
import sqlancer.yugabyte.ysql.gen.YSQLUpdateGenerator;
import sqlancer.yugabyte.ysql.gen.YSQLVacuumGenerator;
import sqlancer.yugabyte.ysql.gen.YSQLViewGenerator;

@AutoService(DatabaseProvider.class)
public class YSQLProvider extends SQLProviderAdapter<YSQLGlobalState, YSQLOptions> {

    // TODO Due to yugabyte problems with parallel DDL we need this lock object
    public static final Object DDL_LOCK = new Object();
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
            nrPerformed = r.getInteger(0, 0);
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
        case TABLEGROUP:
            nrPerformed = r.getInteger(0, 3);
            break;
        case DELETE:
        case RESET_ROLE:
        case VACUUM:
        case SET_CONSTRAINTS:
        case SET:
        case COMMENT_ON:
        case NOTIFY:
        case LISTEN:
        case UNLISTEN:
        case CREATE_SEQUENCE:
        case TRUNCATE:
            nrPerformed = r.getInteger(0, 2);
            break;
        case CREATE_VIEW:
            nrPerformed = r.getInteger(0, 2);
            break;
        case UPDATE:
            nrPerformed = r.getInteger(0, 10);
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
        readFunctions(globalState);
        createTables(globalState, Randomly.fromOptions(4, 5, 6));
        prepareTables(globalState);
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
        synchronized (DDL_LOCK) {
            exceptionLessSleep(5000);

            Connection con = createConnectionSafely(entryURL, username, password);
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
            con.close();
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
        synchronized (DDL_LOCK) {
            boolean prevCreationFailed = false; // small optimization - wait only after failed requests
            while (globalState.getSchema().getDatabaseTables().size() < numTables) {
                if (!prevCreationFailed) {
                    exceptionLessSleep(5000);
                }

                try {
                    String tableName = DBMSCommon.createTableName(globalState.getSchema().getDatabaseTables().size());
                    SQLQueryAdapter createTable = YSQLTableGenerator.generate(tableName, generateOnlyKnown,
                            globalState);
                    globalState.executeStatement(createTable);
                    prevCreationFailed = false;
                } catch (IgnoreMeException e) {
                    prevCreationFailed = true;
                }
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
        if (Randomly.getBoolean() && state.getDbmsSpecificOptions().testCollations) {
            sb.append("WITH ");
            if (Randomly.getBoolean()) {
                sb.append("ENCODING '");
                sb.append(Randomly.fromOptions("utf8"));
                sb.append("' ");
            }

            if (Randomly.getBoolean()) {
                // if (YugabyteBugs.bug11357) {
                // throw new IgnoreMeException();
                // }

                sb.append("COLOCATED = true ");
            }

            for (String lc : Arrays.asList("LC_COLLATE", "LC_CTYPE")) {
                if (!state.getCollates().isEmpty() && Randomly.getBoolean()) {
                    sb.append(String.format(" %s = '%s'", lc, Randomly.fromList(state.getCollates())));
                }
            }
            sb.append(" TEMPLATE template0");

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
        TABLEGROUP(YSQLTableGroupGenerator::create), //
        VACUUM(YSQLVacuumGenerator::create), //
        SET(YSQLSetGenerator::create), // TODO insert yugabyte sets
        SET_CONSTRAINTS((g) -> {
            String sb = "SET CONSTRAINTS ALL " + Randomly.fromOptions("DEFERRED", "IMMEDIATE");
            return new SQLQueryAdapter(sb);
        }), //
        RESET_ROLE((g) -> new SQLQueryAdapter("RESET ROLE")), //
        COMMENT_ON(YSQLCommentGenerator::generate), //
        RESET((g) -> new SQLQueryAdapter("RESET ALL") /*
                                                       * https://www.postgres.org/docs/devel/sql-reset.html TODO: also
                                                       * configuration parameter
                                                       */), //
        NOTIFY(YSQLNotifyGenerator::createNotify), //
        LISTEN((g) -> YSQLNotifyGenerator.createListen()), //
        UNLISTEN((g) -> YSQLNotifyGenerator.createUnlisten()), //
        CREATE_SEQUENCE(YSQLSequenceGenerator::createSequence), //
        CREATE_VIEW(YSQLViewGenerator::create);

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
