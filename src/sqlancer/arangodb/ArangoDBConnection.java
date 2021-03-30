package sqlancer.arangodb;

import com.arangodb.ArangoDB;
import com.arangodb.ArangoDatabase;

import sqlancer.SQLancerDBConnection;

public class ArangoDBConnection implements SQLancerDBConnection {

    private final ArangoDB client;
    private final ArangoDatabase database;

    public ArangoDBConnection(ArangoDB client, ArangoDatabase database) {
        this.client = client;
        this.database = database;
    }

    @Override
    public String getDatabaseVersion() throws Exception {
        return client.getVersion().getVersion();
    }

    @Override
    public void close() throws Exception {
        client.shutdown();
    }

    public ArangoDatabase getDatabase() {
        return database;
    }
}
