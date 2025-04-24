package sqlancer.oxla;

import com.google.auto.service.AutoService;
import sqlancer.*;
import sqlancer.common.DBMSCommon;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLQueryProvider;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.oxla.gen.OxlaTableGenerator;

import java.sql.DriverManager;

// EXISTS
// IN
@AutoService(DatabaseProvider.class)
public class OxlaProvider extends SQLProviderAdapter<OxlaGlobalState, OxlaOptions> {
    protected String entryURL;
    protected String username;
    protected String password;
    protected String host;
    protected Integer port;

    /** Actions performed each time the database is prepared. */
    public enum OxlaAction implements AbstractAction<OxlaGlobalState> {
        TEST_SELECT(state -> new SQLQueryAdapter("SELECT 1"));

        private final SQLQueryProvider<OxlaGlobalState> sqlQueryProvider;

        OxlaAction(SQLQueryProvider<OxlaGlobalState> sqlQueryProvider) {
            this.sqlQueryProvider = sqlQueryProvider;
        }

        @Override
        public SQLQueryAdapter getQuery(OxlaGlobalState state) throws Exception {
            return sqlQueryProvider.getQuery(state);
        }

        public static int mapActions(OxlaGlobalState globalState, OxlaAction action) {
            Randomly randomly = globalState.getRandomly();
            switch (action) {
                case TEST_SELECT:
                    return randomly.getInteger(0, 5);
                default:
                    throw new AssertionError(action);
            }
        }
    }


    public OxlaProvider() {
        super(OxlaGlobalState.class, OxlaOptions.class);
    }

    protected OxlaProvider(Class<OxlaGlobalState> globalClass, Class<OxlaOptions> optionClass) {
        super(globalClass, optionClass);
    }

    @Override
    public void generateDatabase(OxlaGlobalState globalState) throws Exception {
        // Read functions
        SQLQueryAdapter query = new SQLQueryAdapter("SELECT proname, provolatile FROM pg_proc;");
        SQLancerResultSet rs = query.executeAndGet(globalState);
        while (rs.next()) {
            String functionName = rs.getString(1);
            Character functionType = rs.getString(2).charAt(0);
            globalState.functionAndTypes.put(functionName, functionType);
        }

        // Create tables
        final long tableCount = Randomly.getNotCachedInteger(3, 7); // [)
        while (globalState.getSchema().getDatabaseTables().size() < tableCount) {
            String tableName = DBMSCommon.createTableName(globalState.getSchema().getDatabaseTables().size());
            SQLQueryAdapter createTableStatement = OxlaTableGenerator.generate(tableName, globalState);
            globalState.executeStatement(createTableStatement);
        }

        // Prepare tables
        StatementExecutor<OxlaGlobalState, OxlaAction> statementExecutor = new StatementExecutor<>(globalState, OxlaAction.values(), OxlaAction::mapActions, (q) -> {
            if (globalState.getSchema().getDatabaseTables().isEmpty()) {
                throw new IgnoreMeException();
            }
        });
        statementExecutor.executeStatements();
    }

    @Override
    public SQLConnection createDatabase(OxlaGlobalState globalState) throws Exception {
        MainOptions genericOptions = globalState.getOptions();

        username = genericOptions.getUserName();
        password = genericOptions.getPassword();
        host = genericOptions.getHost();
        port = genericOptions.getPort();
        entryURL = globalState.getDbmsSpecificOptions().connectionURL;

        if (entryURL.startsWith("jdbc:")) {
            entryURL = entryURL.substring(5);
        }
        return new SQLConnection(DriverManager.getConnection("jdbc:" + entryURL, username, password));
    }

    @Override
    public String getDBMSName() {
        return "oxla";
    }
}
