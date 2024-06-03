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
import sqlancer.cnosdb.gen.CnosDBInsertGenerator;
import sqlancer.cnosdb.gen.CnosDBTableGenerator;
import sqlancer.cnosdb.query.CnosDBOtherQuery;
import sqlancer.cnosdb.query.CnosDBQueryProvider;
import sqlancer.common.log.LoggableFactory;

@AutoService(DatabaseProvider.class)
public class CnosDBProvider extends ProviderAdapter<CnosDBGlobalState, CnosDBOptions, CnosDBConnection> {

    public CnosDBProvider() {
        super(CnosDBGlobalState.class, CnosDBOptions.class);
    }

    public enum Action implements AbstractAction<CnosDBGlobalState> {
        INSERT(CnosDBInsertGenerator::getQuery);

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

    protected static int mapActions(CnosDBGlobalState globalState, Action a) {
        Randomly r = globalState.getRandomly();
        if (Objects.requireNonNull(a) == Action.INSERT) {
            return r.getInteger(0, globalState.getOptions().getMaxNumberInserts());
        } else {
            throw new AssertionError(a);
        }
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

        String username = globalState.getOptions().getUserName();
        String password = globalState.getOptions().getPassword();
        String host = globalState.getOptions().getHost();
        int port = globalState.getOptions().getPort();
        String databaseName = globalState.getDatabaseName();
        CnosDBClient client = new CnosDBClient(host, port, username, password, databaseName);
        CnosDBConnection connection = new CnosDBConnection(client);
        client.execute("DROP DATABASE IF EXISTS " + databaseName);
        globalState.getState().logStatement("DROP DATABASE IF EXISTS " + databaseName);
        client.execute("CREATE DATABASE " + databaseName);
        globalState.getState().logStatement("CREATE DATABASE " + databaseName);

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

    @Override
    public String getDBMSName() {
        return "CnosDB".toLowerCase();
    }

    @Override
    public LoggableFactory getLoggableFactory() {
        return new CnosDBLoggableFactory();
    }
}
