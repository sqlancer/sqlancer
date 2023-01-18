package sqlancer.cnosdb.client;

import sqlancer.SQLancerDBConnection;

import java.io.IOException;

public class CnosDBConnection implements SQLancerDBConnection {
    private final CnosDBClient client;

    public CnosDBConnection(CnosDBClient client) {
        this.client = client;
    }

    @Override
    public String getDatabaseVersion() throws Exception {
        return client.ping();
    }

    public CnosDBClient getClient() {
        return client;
    }

    @Override
    public void close() throws IOException {
        client.close();
    }
}
