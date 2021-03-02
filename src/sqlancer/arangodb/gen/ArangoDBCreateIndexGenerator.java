package sqlancer.arangodb.gen;

import sqlancer.arangodb.ArangoDBProvider;
import sqlancer.arangodb.ArangoDBQueryAdapter;
import sqlancer.arangodb.ArangoDBSchema;
import sqlancer.arangodb.query.ArangoDBCreateIndexQuery;

public final class ArangoDBCreateIndexGenerator {
    private ArangoDBCreateIndexGenerator() {

    }

    public static ArangoDBQueryAdapter getQuery(ArangoDBProvider.ArangoDBGlobalState globalState) {
        ArangoDBSchema.ArangoDBColumn column = null;
        while (column == null) {
            ArangoDBSchema.ArangoDBTable randomTable = globalState.getSchema().getRandomTable();
            column = randomTable.getRandomColumn();
        }
        return new ArangoDBCreateIndexQuery(column);
    }
}
