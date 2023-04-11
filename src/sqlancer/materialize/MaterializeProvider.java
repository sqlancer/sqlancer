package sqlancer.materialize;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
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
import sqlancer.materialize.MaterializeOptions.MaterializeOracleFactory;
import sqlancer.materialize.gen.MaterializeDeleteGenerator;
import sqlancer.materialize.gen.MaterializeDropIndexGenerator;
import sqlancer.materialize.gen.MaterializeIndexGenerator;
import sqlancer.materialize.gen.MaterializeInsertGenerator;
import sqlancer.materialize.gen.MaterializeTableGenerator;
import sqlancer.materialize.gen.MaterializeUpdateGenerator;
import sqlancer.materialize.gen.MaterializeViewGenerator;

// EXISTS
// IN
@AutoService(DatabaseProvider.class)
public class MaterializeProvider extends SQLProviderAdapter<MaterializeGlobalState, MaterializeOptions> {

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
    protected String extensionsList;

    public MaterializeProvider() {
        super(MaterializeGlobalState.class, MaterializeOptions.class);
    }

    protected MaterializeProvider(Class<MaterializeGlobalState> globalClass, Class<MaterializeOptions> optionClass) {
        super(globalClass, optionClass);
    }

    public enum Action implements AbstractAction<MaterializeGlobalState> {
        DELETE(MaterializeDeleteGenerator::create), //
        DROP_INDEX(MaterializeDropIndexGenerator::create), //
        INSERT(MaterializeInsertGenerator::insert), //
        UPDATE(MaterializeUpdateGenerator::create), //
        CREATE_INDEX(MaterializeIndexGenerator::generate), //
        CREATE_VIEW(MaterializeViewGenerator::create);

        private final SQLQueryProvider<MaterializeGlobalState> sqlQueryProvider;

        Action(SQLQueryProvider<MaterializeGlobalState> sqlQueryProvider) {
            this.sqlQueryProvider = sqlQueryProvider;
        }

        @Override
        public SQLQueryAdapter getQuery(MaterializeGlobalState state) throws Exception {
            return sqlQueryProvider.getQuery(state);
        }
    }

    protected static int mapActions(MaterializeGlobalState globalState, Action a) {
        Randomly r = globalState.getRandomly();
        int nrPerformed;
        switch (a) {
        case CREATE_INDEX:
            nrPerformed = r.getInteger(0, 3);
            break;
        case DROP_INDEX:
            nrPerformed = r.getInteger(0, 5);
            break;
        case DELETE:
            nrPerformed = r.getInteger(0, 5);
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
    public void generateDatabase(MaterializeGlobalState globalState) throws Exception {
        readFunctions(globalState);
        createTables(globalState, Randomly.fromOptions(4, 5, 6));
        prepareTables(globalState);

        extensionsList = globalState.getDbmsSpecificOptions().extensions;
        if (!extensionsList.isEmpty()) {
            String[] extensionNames = extensionsList.split(",");

            /*
             * To avoid of a test interference with an extension objects, create them in a separate schema. Of course,
             * they must be truly relocatable.
             */
            globalState.executeStatement(new SQLQueryAdapter("CREATE SCHEMA extensions;", true));
            for (int i = 0; i < extensionNames.length; i++) {
                globalState.executeStatement(new SQLQueryAdapter(
                        "CREATE EXTENSION " + extensionNames[i] + " WITH SCHEMA extensions;", true));
            }
        }
    }

    @Override
    public SQLConnection createDatabase(MaterializeGlobalState globalState) throws SQLException {
        if (globalState.getDbmsSpecificOptions().getTestOracleFactory().stream()
                .anyMatch((o) -> o == MaterializeOracleFactory.PQS)) {
            generateOnlyKnown = true;
        }

        username = globalState.getOptions().getUserName();
        password = globalState.getOptions().getPassword();
        host = globalState.getOptions().getHost();
        port = globalState.getOptions().getPort();
        entryPath = "/test";
        entryURL = globalState.getDbmsSpecificOptions().connectionURL;
        // trim URL to exclude "jdbc:"
        if (entryURL.startsWith("jdbc:")) {
            entryURL = entryURL.substring(5);
        }
        String entryDatabaseName = entryPath.substring(1);
        databaseName = globalState.getDatabaseName();

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
            entryURL = String.format("%s://%s:%d/%s", uri.getScheme(), host, port, entryDatabaseName);
        } catch (URISyntaxException e) {
            throw new AssertionError(e);
        }
        Connection con = DriverManager.getConnection("jdbc:" + entryURL, username, password);
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
        if (globalState.getDbmsSpecificOptions().setMaxTablesMVs) {
            Connection conMzSystem = DriverManager.getConnection("jdbc:postgresql://localhost:6877/materialize",
                    "mz_system", "materialize");
            try (Statement s = conMzSystem.createStatement()) {
                s.execute("ALTER SYSTEM SET max_tables TO 1000");
            }
            try (Statement s = conMzSystem.createStatement()) {
                s.execute("ALTER SYSTEM SET max_materialized_views TO 1000");
            }
            conMzSystem.close();
        }
        int databaseIndex = entryURL.indexOf(entryDatabaseName);
        String preDatabaseName = entryURL.substring(0, databaseIndex);
        String postDatabaseName = entryURL.substring(databaseIndex + entryDatabaseName.length());
        testURL = preDatabaseName + databaseName + postDatabaseName;
        globalState.getState().logStatement(String.format("\\c %s;", databaseName));

        con = DriverManager.getConnection("jdbc:" + testURL, username, password);
        // Serializable transaction isolation is much faster than Strict
        // Serializable and should guarantee enough for SQLancer:
        // https://materialize.com/docs/overview/isolation-level/
        try (Statement s = con.createStatement()) {
            s.execute("SET transaction_isolation = 'SERIALIZABLE'");
        }
        return new SQLConnection(con);
    }

    protected void readFunctions(MaterializeGlobalState globalState) throws SQLException {
        // ERROR: column "provolatile" does not exist
        SQLQueryAdapter query = new SQLQueryAdapter("SELECT proname, 1 FROM pg_proc;");
        SQLancerResultSet rs = query.executeAndGet(globalState);
        while (rs.next()) {
            String functionName = rs.getString(1);
            Character functionType = rs.getString(2).charAt(0);
            globalState.addFunctionAndType(functionName, functionType);
        }
    }

    protected void createTables(MaterializeGlobalState globalState, int numTables) throws Exception {
        while (globalState.getSchema().getDatabaseTables().size() < numTables) {
            try {
                String tableName = DBMSCommon.createTableName(globalState.getSchema().getDatabaseTables().size());
                SQLQueryAdapter createTable = MaterializeTableGenerator.generate(tableName, globalState.getSchema(),
                        generateOnlyKnown, globalState);
                globalState.executeStatement(createTable);
            } catch (IgnoreMeException e) {

            }
        }
    }

    protected void prepareTables(MaterializeGlobalState globalState) throws Exception {
        StatementExecutor<MaterializeGlobalState, Action> se = new StatementExecutor<>(globalState, Action.values(),
                MaterializeProvider::mapActions, (q) -> {
                    if (globalState.getSchema().getDatabaseTables().isEmpty()) {
                        throw new IgnoreMeException();
                    }
                });
        se.executeStatements();
        globalState.executeStatement(new SQLQueryAdapter("COMMIT", true));
        globalState.executeStatement(new SQLQueryAdapter("SET SESSION statement_timeout = 5000;\n"));
    }

    private String getCreateDatabaseCommand(MaterializeGlobalState state) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE DATABASE " + databaseName + " ");
        if (Randomly.getBoolean() && ((MaterializeOptions) state.getDbmsSpecificOptions()).testCollations) {
            for (String lc : Arrays.asList("LC_COLLATE", "LC_CTYPE")) {
                if (!state.getCollates().isEmpty() && Randomly.getBoolean()) {
                    sb.append(String.format(" %s = '%s'", lc, Randomly.fromList(state.getCollates())));
                }
            }
        }
        return sb.toString();
    }

    @Override
    public String getDBMSName() {
        return "materialize";
    }

    @Override
    public String getQueryPlan(String selectStr, MaterializeGlobalState globalState) throws Exception {
        String queryPlan = "";
        String explainQuery = "EXPLAIN OPTIMIZED PLAN FOR " + selectStr;
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
        SQLancerResultSet rs = q.executeAndGet(globalState);
        if (rs != null) {
            while (rs.next()) {
                String line;
                BufferedReader bufReader = new BufferedReader(new StringReader(rs.getString(1)));
                while ((line = bufReader.readLine()) != null) {
                    String targetQueryPlan = line.trim() + ";"; // Unify format
                    if (targetQueryPlan.startsWith("Explained Query:")) {
                        continue;
                    }
                    if (afterProjection) {
                        afterProjection = false;
                        continue;
                    }
                    if (targetQueryPlan.startsWith("Project")) {
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
        }

        return queryPlan;
    }

    @Override
    protected double[] initializeWeightedAverageReward() {
        return new double[Action.values().length];
    }

    @Override
    protected void executeMutator(int index, MaterializeGlobalState globalState) throws Exception {
        SQLQueryAdapter queryMutateTable = Action.values()[index].getQuery(globalState);
        globalState.executeStatement(queryMutateTable);
    }
}
