package sqlancer.arangodb;

import java.util.ArrayList;
import java.util.List;

import com.arangodb.ArangoDB;
import com.arangodb.ArangoDatabase;
import com.google.auto.service.AutoService;

import sqlancer.AbstractAction;
import sqlancer.DatabaseProvider;
import sqlancer.ExecutionTimer;
import sqlancer.GlobalState;
import sqlancer.IgnoreMeException;
import sqlancer.ProviderAdapter;
import sqlancer.Randomly;
import sqlancer.StatementExecutor;
import sqlancer.arangodb.gen.ArangoDBCreateIndexGenerator;
import sqlancer.arangodb.gen.ArangoDBInsertGenerator;
import sqlancer.arangodb.gen.ArangoDBTableGenerator;
import sqlancer.common.log.LoggableFactory;
import sqlancer.common.query.Query;

@AutoService(DatabaseProvider.class)
public class ArangoDBProvider
        extends ProviderAdapter<ArangoDBProvider.ArangoDBGlobalState, ArangoDBOptions, ArangoDBConnection> {

    public ArangoDBProvider() {
        super(ArangoDBGlobalState.class, ArangoDBOptions.class);
    }

    enum Action implements AbstractAction<ArangoDBGlobalState> {
        INSERT(ArangoDBInsertGenerator::getQuery), CREATE_INDEX(ArangoDBCreateIndexGenerator::getQuery);

        private final ArangoDBQueryProvider<ArangoDBGlobalState> queryProvider;

        Action(ArangoDBQueryProvider<ArangoDBGlobalState> queryProvider) {
            this.queryProvider = queryProvider;
        }

        @Override
        public Query<?> getQuery(ArangoDBGlobalState globalState) throws Exception {
            return queryProvider.getQuery(globalState);
        }
    }

    private static int mapActions(ArangoDBGlobalState globalState, Action a) {
        Randomly r = globalState.getRandomly();
        switch (a) {
        case INSERT:
            return r.getInteger(0, globalState.getOptions().getMaxNumberInserts());
        case CREATE_INDEX:
            return r.getInteger(0, globalState.getDbmsSpecificOptions().maxNumberIndexes);
        default:
            throw new AssertionError(a);
        }
    }

    public static class ArangoDBGlobalState extends GlobalState<ArangoDBOptions, ArangoDBSchema, ArangoDBConnection> {

        private final List<ArangoDBSchema.ArangoDBTable> schemaTables = new ArrayList<>();

        public void addTable(ArangoDBSchema.ArangoDBTable table) {
            schemaTables.add(table);
        }

        @Override
        protected void executeEpilogue(Query<?> q, boolean success, ExecutionTimer timer) throws Exception {
            boolean logExecutionTime = getOptions().logExecutionTime();
            if (success && getOptions().printSucceedingStatements()) {
                System.out.println(q.getLogString());
            }
            if (logExecutionTime) {
                getLogger().writeCurrent("//" + timer.end().asString());
            }
            if (q.couldAffectSchema()) {
                updateSchema();
            }
        }

        @Override
        protected ArangoDBSchema readSchema() throws Exception {
            return new ArangoDBSchema(schemaTables);
        }
    }

    @Override
    protected void checkViewsAreValid(ArangoDBGlobalState globalState) {

    }

    @Override
    public void generateDatabase(ArangoDBGlobalState globalState) throws Exception {
        for (int i = 0; i < Randomly.fromOptions(4, 5, 6); i++) {
            boolean success;
            do {
                ArangoDBQueryAdapter queryAdapter = new ArangoDBTableGenerator().getQuery(globalState);
                success = globalState.executeStatement(queryAdapter);
            } while (!success);
        }
        StatementExecutor<ArangoDBGlobalState, Action> se = new StatementExecutor<>(globalState, Action.values(),
                ArangoDBProvider::mapActions, (q) -> {
                    if (globalState.getSchema().getDatabaseTables().isEmpty()) {
                        throw new IgnoreMeException();
                    }
                });
        se.executeStatements();
    }

    @Override
    public ArangoDBConnection createDatabase(ArangoDBGlobalState globalState) throws Exception {
        ArangoDB arangoDB = new ArangoDB.Builder().user(globalState.getOptions().getUserName())
                .password(globalState.getOptions().getPassword()).build();
        ArangoDatabase database = arangoDB.db(globalState.getDatabaseName());
        try {
            database.drop();
            // When the database does not exist, an ArangoDB exception is thrown. Since we are not sure
            // if this is the first time the database is used, the simplest is dropping it and ignoring
            // the exception.
        } catch (Exception ignored) {

        }
        arangoDB.createDatabase(globalState.getDatabaseName());
        database = arangoDB.db(globalState.getDatabaseName());
        return new ArangoDBConnection(arangoDB, database);
    }

    @Override
    public String getDBMSName() {
        return "arangodb";
    }

    @Override
    public LoggableFactory getLoggableFactory() {
        return new ArangoDBLoggableFactory();
    }
}
