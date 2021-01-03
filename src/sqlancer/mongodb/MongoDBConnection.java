package sqlancer.mongodb;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;

import sqlancer.SQLancerDBConnection;

public class MongoDBConnection implements SQLancerDBConnection {

    private final MongoClient client;
    private final MongoDatabase database;

    public MongoDBConnection(MongoClient client, MongoDatabase database) {
        this.client = client;
        this.database = database;
    }

    @Override
    public String getDatabaseVersion() throws Exception {
        return "4.4, Java API 4.1";
    }

    @Override
    public void close() throws Exception {
        client.close();
    }

    public MongoDatabase getDatabase() {
        return database;
    }
}
