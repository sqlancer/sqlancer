package sqlancer.cosmos;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;

import sqlancer.IgnoreMeException;
import sqlancer.ProviderAdapter;
import sqlancer.Randomly;
import sqlancer.StatementExecutor;
import sqlancer.common.log.LoggableFactory;
import sqlancer.mongodb.MongoDBConnection;
import sqlancer.mongodb.MongoDBLoggableFactory;
import sqlancer.mongodb.MongoDBOptions;
import sqlancer.mongodb.MongoDBQueryAdapter;
import sqlancer.mongodb.gen.MongoDBTableGenerator;

public class CosmosProvider extends
        ProviderAdapter<sqlancer.mongodb.MongoDBProvider.MongoDBGlobalState, MongoDBOptions, MongoDBConnection> {

    public CosmosProvider() {
        super(sqlancer.mongodb.MongoDBProvider.MongoDBGlobalState.class, MongoDBOptions.class);
    }

    @Override
    public void generateDatabase(sqlancer.mongodb.MongoDBProvider.MongoDBGlobalState globalState) throws Exception {
        for (int i = 0; i < Randomly.fromOptions(4, 5, 6); i++) {
            boolean success;
            do {
                MongoDBQueryAdapter query = new MongoDBTableGenerator(globalState).getQuery(globalState);
                success = globalState.executeStatement(query);
            } while (!success);
        }
        StatementExecutor<sqlancer.mongodb.MongoDBProvider.MongoDBGlobalState, sqlancer.mongodb.MongoDBProvider.Action> se = new StatementExecutor<>(
                globalState, sqlancer.mongodb.MongoDBProvider.Action.values(),
                sqlancer.mongodb.MongoDBProvider::mapActions, (q) -> {
                    if (globalState.getSchema().getDatabaseTables().isEmpty()) {
                        throw new IgnoreMeException();
                    }
                });
        se.executeStatements();
    }

    @Override
    public MongoDBConnection createDatabase(sqlancer.mongodb.MongoDBProvider.MongoDBGlobalState globalState)
            throws Exception {
        String connectionString = "";
        if (connectionString.equals("")) {
            throw new AssertionError("Please set connection string for cosmos database, located in CosmosProvider");
        }
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(connectionString)).build();
        MongoClient mongoClient = MongoClients.create(settings);
        MongoDatabase database = mongoClient.getDatabase(globalState.getDatabaseName());
        database.drop();
        return new MongoDBConnection(mongoClient, database);
    }

    @Override
    public String getDBMSName() {
        return "cosmos";
    }

    @Override
    public LoggableFactory getLoggableFactory() {
        return new MongoDBLoggableFactory();
    }

    @Override
    protected void checkViewsAreValid(sqlancer.mongodb.MongoDBProvider.MongoDBGlobalState globalState) {
    }
}
