package sqlancer.mongodb;

import java.util.ArrayList;
import java.util.List;

import com.google.auto.service.AutoService;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;

import sqlancer.AbstractAction;
import sqlancer.DatabaseProvider;
import sqlancer.ExecutionTimer;
import sqlancer.GlobalState;
import sqlancer.IgnoreMeException;
import sqlancer.ProviderAdapter;
import sqlancer.Randomly;
import sqlancer.StatementExecutor;
import sqlancer.common.log.LoggableFactory;
import sqlancer.common.query.Query;
import sqlancer.mongodb.MongoDBSchema.MongoDBTable;
import sqlancer.mongodb.gen.MongoDBIndexGenerator;
import sqlancer.mongodb.gen.MongoDBInsertGenerator;
import sqlancer.mongodb.gen.MongoDBTableGenerator;

@AutoService(DatabaseProvider.class)
public class MongoDBProvider
        extends ProviderAdapter<MongoDBProvider.MongoDBGlobalState, MongoDBOptions, MongoDBConnection> {

    public MongoDBProvider() {
        super(MongoDBGlobalState.class, MongoDBOptions.class);
    }

    public enum Action implements AbstractAction<MongoDBGlobalState> {
        INSERT(MongoDBInsertGenerator::getQuery), CREATE_INDEX(MongoDBIndexGenerator::getQuery);

        private final MongoDBQueryProvider<MongoDBGlobalState> queryProvider;

        Action(MongoDBQueryProvider<MongoDBGlobalState> queryProvider) {
            this.queryProvider = queryProvider;
        }

        @Override
        public Query<MongoDBConnection> getQuery(MongoDBGlobalState globalState) throws Exception {
            return queryProvider.getQuery(globalState);
        }
    }

    public static int mapActions(MongoDBGlobalState globalState, Action a) {
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

    public static class MongoDBGlobalState extends GlobalState<MongoDBOptions, MongoDBSchema, MongoDBConnection> {

        private final List<MongoDBTable> schemaTables = new ArrayList<>();

        public void addTable(MongoDBTable table) {
            schemaTables.add(table);
        }

        @Override
        protected void executeEpilogue(Query<?> q, boolean success, ExecutionTimer timer) throws Exception {
            boolean logExecutionTime = getOptions().logExecutionTime();
            if (success && getOptions().printSucceedingStatements()) {
                System.out.println(q.getLogString());
            }
            if (logExecutionTime) {
                getLogger().writeCurrent("// " + timer.end().asString());
            }
            if (q.couldAffectSchema()) {
                updateSchema();
            }
        }

        @Override
        protected MongoDBSchema readSchema() throws Exception {
            return new MongoDBSchema(schemaTables);
        }
    }

    @Override
    public void generateDatabase(MongoDBGlobalState globalState) throws Exception {
        for (int i = 0; i < Randomly.fromOptions(4, 5, 6); i++) {
            boolean success;
            do {
                MongoDBQueryAdapter query = new MongoDBTableGenerator(globalState).getQuery(globalState);
                success = globalState.executeStatement(query);
            } while (!success);
        }
        StatementExecutor<MongoDBGlobalState, Action> se = new StatementExecutor<>(globalState, Action.values(),
                MongoDBProvider::mapActions, (q) -> {
                    if (globalState.getSchema().getDatabaseTables().isEmpty()) {
                        throw new IgnoreMeException();
                    }
                });
        se.executeStatements();
    }

    @Override
    public MongoDBConnection createDatabase(MongoDBGlobalState globalState) throws Exception {
        MongoClient mongoClient = MongoClients.create();
        MongoDatabase database = mongoClient.getDatabase(globalState.getDatabaseName());
        database.drop();
        return new MongoDBConnection(mongoClient, database);
    }

    @Override
    public String getDBMSName() {
        return "mongodb";
    }

    @Override
    public LoggableFactory getLoggableFactory() {
        return new MongoDBLoggableFactory();
    }

    @Override
    protected void checkViewsAreValid(MongoDBGlobalState globalState) {
    }
}
