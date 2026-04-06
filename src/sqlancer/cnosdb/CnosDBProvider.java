package sqlancer.cnosdb;

import java.util.Objects;

import com.google.auto.service.AutoService;

import sqlancer.AbstractAction;
import sqlancer.DatabaseProvider;
import sqlancer.IgnoreMeException;
import sqlancer.ProviderAdapter;
import sqlancer.Randomly;
import sqlancer.StatementExecutor;
import sqlancer.cnosdb.client.CnosDBClient;
import sqlancer.cnosdb.client.CnosDBConnection;
import sqlancer.cnosdb.client.CnosDBException;
import sqlancer.cnosdb.gen.CnosDBInsertGenerator;
import sqlancer.cnosdb.gen.CnosDBTableGenerator;
import sqlancer.cnosdb.query.CnosDBOtherQuery;
import sqlancer.cnosdb.query.CnosDBQueryProvider;
import sqlancer.common.log.LoggableFactory;

@AutoService(DatabaseProvider.class)
public class CnosDBProvider extends ProviderAdapter<CnosDBGlobalState, CnosDBOptions, CnosDBConnection> {

    private static final int DB_READY_RETRIES = 30;
    private static final int DB_READY_SLEEP_MS = 1000;

    protected String username;
    protected String password;
    protected String host;
    protected int port;
    protected String databaseName;

    public CnosDBProvider() {
        super(CnosDBGlobalState.class, CnosDBOptions.class);
    }

    protected CnosDBProvider(Class<CnosDBGlobalState> globalClass, Class<CnosDBOptions> optionClass) {
        super(globalClass, optionClass);
    }

    protected static int mapActions(CnosDBGlobalState globalState, Action a) {
        Randomly r = globalState.getRandomly();
        int nrPerformed;
        if (Objects.requireNonNull(a) == Action.INSERT) {
            nrPerformed = r.getInteger(0, globalState.getOptions().getMaxNumberInserts());
        } else {
            throw new AssertionError(a);
        }
        return nrPerformed;

    }

    @Override
    protected void checkViewsAreValid(CnosDBGlobalState globalState) {
    }

    @Override
    public void generateDatabase(CnosDBGlobalState globalState) throws Exception {
        createTables(globalState, Randomly.fromOptions(4, 5, 6));
        prepareTables(globalState);

    }

    @Override
    public CnosDBConnection createDatabase(CnosDBGlobalState globalState) throws Exception {

        username = globalState.getOptions().getUserName();
        password = globalState.getOptions().getPassword();
        host = globalState.getOptions().getHost();
        port = globalState.getOptions().getPort();
        databaseName = globalState.getDatabaseName();
        // Use a client connected to "public" for DROP/CREATE operations,
        // since CnosDB cannot drop a database from its own connection context.
        CnosDBClient adminClient = new CnosDBClient(host, port, username, password, "public");
        dropAndCreateDatabase(adminClient, databaseName);
        globalState.getState().logStatement("DROP DATABASE IF EXISTS " + databaseName);
        globalState.getState().logStatement("CREATE DATABASE " + databaseName);
        adminClient.close();

        CnosDBClient client = new CnosDBClient(host, port, username, password, databaseName);
        executeWithRetry(client, "SELECT 1");
        CnosDBConnection connection = new CnosDBConnection(client);
        return connection;
    }

    protected void createTables(CnosDBGlobalState globalState, int numTables) throws Exception {
        while (globalState.getSchema().getDatabaseTables().size() < numTables) {
            String tableName = String.format("m%d", globalState.getSchema().getDatabaseTables().size());
            CnosDBOtherQuery createTable = CnosDBTableGenerator.generate(tableName);
            globalState.executeStatement(createTable);
        }
    }

    protected void prepareTables(CnosDBGlobalState globalState) throws Exception {
        StatementExecutor<CnosDBGlobalState, Action> se = new StatementExecutor<>(globalState, Action.values(),
                CnosDBProvider::mapActions, (q) -> {
                    if (globalState.getSchema().getDatabaseTables().isEmpty()) {
                        throw new IgnoreMeException();
                    }
                });
        se.executeStatements();
    }

    /**
     * Drops and recreates a database, retrying the entire DROP+CREATE sequence on transient errors.
     *
     * CnosDB's Tskv index storage may not be fully ready even after the HTTP API responds to simple queries. DDL
     * operations can fail with: {@code "grpc client request error: Tskv: Index: index storage error: Resource
     * temporarily unavailable (os error 11)"}. DROP DATABASE may also appear to succeed but not yet propagate, causing
     * CREATE DATABASE to fail with {@code "Database already exists"}. Retrying just the CREATE is futile in that case,
     * so we retry the DROP+CREATE pair together.
     */
    private static void dropAndCreateDatabase(CnosDBClient client, String databaseName) throws Exception {
        String dropQuery = "DROP DATABASE IF EXISTS " + databaseName;
        String createQuery = "CREATE DATABASE " + databaseName;
        for (int i = 0; i < DB_READY_RETRIES; i++) {
            try {
                client.execute(dropQuery);
                client.execute(createQuery);
                return;
            } catch (CnosDBException e) {
                boolean isRetryable = e.getMessage().contains("Resource temporarily unavailable")
                        || e.getMessage().contains("already exists");
                if (!isRetryable || i == DB_READY_RETRIES - 1) {
                    throw e;
                }
                Thread.sleep(DB_READY_SLEEP_MS);
            }
        }
    }

    /**
     * Executes a query with retries to work around CnosDB storage layer initialization delays.
     *
     * A newly created database may not be immediately queryable, causing {@code "Database not found"} errors.
     */
    private static void executeWithRetry(CnosDBClient client, String query) throws Exception {
        for (int i = 0; i < DB_READY_RETRIES; i++) {
            try {
                client.execute(query);
                return;
            } catch (CnosDBException e) {
                boolean isRetryable = e.getMessage().contains("Database not found");
                if (!isRetryable || i == DB_READY_RETRIES - 1) {
                    throw e;
                }
                Thread.sleep(DB_READY_SLEEP_MS);
            }
        }
    }

    @Override
    public String getDBMSName() {
        return "CnosDB".toLowerCase();
    }

    @Override
    public LoggableFactory getLoggableFactory() {
        return new CnosDBLoggableFactory();
    }

    public enum Action implements AbstractAction<CnosDBGlobalState> {
        INSERT(CnosDBInsertGenerator::insert);

        private final CnosDBQueryProvider<CnosDBGlobalState> sqlQueryProvider;

        Action(CnosDBQueryProvider<CnosDBGlobalState> sqlQueryProvider) {
            this.sqlQueryProvider = sqlQueryProvider;
        }

        @Override
        public CnosDBOtherQuery getQuery(CnosDBGlobalState state) throws Exception {
            return new CnosDBOtherQuery(sqlQueryProvider.getQuery(state).getQueryString(),
                    CnosDBExpectedError.expectedErrors());
        }
    }

}
