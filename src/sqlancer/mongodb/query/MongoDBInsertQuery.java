package sqlancer.mongodb.query;

import org.bson.BsonDateTime;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.types.ObjectId;

import com.mongodb.client.result.InsertOneResult;

import sqlancer.GlobalState;
import sqlancer.Main;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.mongodb.MongoDBConnection;
import sqlancer.mongodb.MongoDBQueryAdapter;
import sqlancer.mongodb.MongoDBSchema.MongoDBTable;

public class MongoDBInsertQuery extends MongoDBQueryAdapter {
    boolean excluded;
    private final MongoDBTable table;
    private final Document documentToBeInserted;

    public MongoDBInsertQuery(MongoDBTable table, Document documentToBeInserted) {
        this.table = table;
        this.documentToBeInserted = documentToBeInserted;
        this.excluded = false;
    }

    @Override
    public String getLogString() {
        StringBuilder sb = new StringBuilder();
        sb.append("db." + table.getName() + ".insert({");
        String helper = "";
        for (String key : documentToBeInserted.keySet()) {
            sb.append(helper);
            helper = ", ";
            if (documentToBeInserted.get(key) instanceof ObjectId) {
                continue;
            }
            Object value = documentToBeInserted.get(key);
            sb.append(key);
            sb.append(": ");
            sb.append(getStringRepresentation(value));
        }
        sb.append("})\n");

        return sb.toString();
    }

    private String getStringRepresentation(Object value) {
        if (value instanceof Double) {
            return String.valueOf(value);
        } else if (value instanceof Integer) {
            return "NumberInt(" + value + ")";
        } else if (value instanceof String) {
            return "\"" + value + "\"";
        } else if (value instanceof BsonDateTime) {
            return "new Date(" + ((BsonDateTime) value).getValue() + ")";
        } else if (value instanceof BsonTimestamp) {
            return "Timestamp(" + ((BsonTimestamp) value).getValue() + ",1)";
        } else if (value instanceof Boolean) {
            return String.valueOf(value);
        } else if (value == null) {
            return "null";
        } else {
            throw new IllegalStateException();
        }
    }

    @Override
    public boolean couldAffectSchema() {
        return true;
    }

    @Override
    public <G extends GlobalState<?, ?, MongoDBConnection>> boolean execute(G globalState, String... fills)
            throws Exception {
        Main.nrSuccessfulActions.addAndGet(1);
        InsertOneResult result = globalState.getConnection().getDatabase().getCollection(table.getName())
                .insertOne(documentToBeInserted);
        return result.wasAcknowledged();
    }

    @Override
    public ExpectedErrors getExpectedErrors() {
        return new ExpectedErrors();
    }
}
