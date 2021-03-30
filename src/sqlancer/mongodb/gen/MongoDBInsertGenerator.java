package sqlancer.mongodb.gen;

import org.bson.Document;

import sqlancer.mongodb.MongoDBProvider.MongoDBGlobalState;
import sqlancer.mongodb.MongoDBQueryAdapter;
import sqlancer.mongodb.MongoDBSchema.MongoDBTable;
import sqlancer.mongodb.query.MongoDBInsertQuery;

public final class MongoDBInsertGenerator {

    private final MongoDBGlobalState globalState;

    private MongoDBInsertGenerator(MongoDBGlobalState globalState) {
        this.globalState = globalState;
    }

    public static MongoDBQueryAdapter getQuery(MongoDBGlobalState globalState) {
        return new MongoDBInsertGenerator(globalState).generate();
    }

    public MongoDBQueryAdapter generate() {
        Document result = new Document();
        MongoDBTable table = globalState.getSchema().getRandomTable();
        MongoDBConstantGenerator constantGenerator = new MongoDBConstantGenerator(globalState);

        for (int i = 0; i < table.getColumns().size(); i++) {
            if (!globalState.getDmbsSpecificOptions().testRandomTypes) {
                constantGenerator.addRandomConstantWithType(result, table.getColumns().get(i).getName(),
                        table.getColumns().get(i).getType());
            } else {
                constantGenerator.addRandomConstant(result, table.getColumns().get(i).getName());
            }
        }

        return new MongoDBInsertQuery(table, result);
    }
}
