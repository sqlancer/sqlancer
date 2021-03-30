package sqlancer.mongodb.gen;

import java.util.List;

import sqlancer.Randomly;
import sqlancer.mongodb.MongoDBProvider.MongoDBGlobalState;
import sqlancer.mongodb.MongoDBQueryAdapter;
import sqlancer.mongodb.MongoDBSchema.MongoDBColumn;
import sqlancer.mongodb.MongoDBSchema.MongoDBTable;
import sqlancer.mongodb.query.MongoDBCreateIndexQuery;

public final class MongoDBIndexGenerator {
    private MongoDBIndexGenerator() {
    }

    public static MongoDBQueryAdapter getQuery(MongoDBGlobalState globalState) {
        MongoDBTable randomTable = globalState.getSchema().getRandomTable();
        List<MongoDBColumn> columns = Randomly.nonEmptySubset(randomTable.getColumns());
        MongoDBCreateIndexQuery createIndexQuery = new MongoDBCreateIndexQuery(randomTable);
        for (MongoDBColumn column : columns) {
            createIndexQuery.addIndex(column.getName(), Randomly.getBoolean());
        }
        return createIndexQuery;
    }
}
