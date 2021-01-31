package sqlancer.mongodb.query;

import java.util.ArrayList;
import java.util.List;

import org.bson.BsonType;
import org.bson.conversions.Bson;

import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ValidationOptions;

import sqlancer.GlobalState;
import sqlancer.Main;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.mongodb.MongoDBConnection;
import sqlancer.mongodb.MongoDBQueryAdapter;

public class MongoDBCreateTableQuery extends MongoDBQueryAdapter {

    private final String tableName;
    private Bson validationFilter;
    private final List<String> logRequiredList;
    private final List<String> logPropertiesList;

    public MongoDBCreateTableQuery(String tableName) {
        this.tableName = tableName;
        this.validationFilter = null;
        logRequiredList = new ArrayList<>();
        logPropertiesList = new ArrayList<>();
    }

    @Override
    public boolean couldAffectSchema() {
        return true;
    }

    @Override
    public <G extends GlobalState<?, ?, MongoDBConnection>> boolean execute(G globalState, String... fills)
            throws Exception {
        ValidationOptions collOptions = new ValidationOptions().validator(this.validationFilter);
        Main.nrSuccessfulActions.addAndGet(1);
        globalState.getConnection().getDatabase().createCollection(tableName,
                new CreateCollectionOptions().validationOptions(collOptions));
        return true;
    }

    @Override
    public ExpectedErrors getExpectedErrors() {
        return new ExpectedErrors();
    }

    @Override
    public String getLogString() {
        String helper = "";
        StringBuilder sb = new StringBuilder();
        sb.append("db.createCollection(\"").append(tableName).append("\", {\n");

        if (!logPropertiesList.isEmpty()) {
            sb.append("validator: {");
            sb.append("$jsonSchema: {");
            sb.append("bsonType:\"object\",");
            sb.append("required: [\n");
            for (String req : logRequiredList) {
                sb.append(helper);
                helper = ",";
                sb.append(req);
            }
            sb.append("],");
            sb.append("properties: {\n");
            for (String prop : logPropertiesList) {
                sb.append(prop);
            }
            sb.append("}}}})");
        } else {
            sb.append("})");
        }

        return sb.toString();
    }

    public void addValidation(String columnName, BsonType type) {
        Bson nameFilter = Filters.exists(columnName);
        Bson typeFilter = Filters.type(columnName, type);

        if (validationFilter == null) {
            validationFilter = Filters.and(nameFilter, typeFilter);
        } else {
            validationFilter = Filters.and(validationFilter, Filters.and(nameFilter, typeFilter));
        }

        logRequiredList.add("\"" + columnName + "\"");
        logPropertiesList.add(columnName + ": { bsonType:\"" + bsonTypeToString(type) + "\"},\n");
    }

    public String bsonTypeToString(BsonType type) {
        switch (type) {
        case DOUBLE:
            return "double";
        case STRING:
            return "string";
        case BOOLEAN:
            return "bool";
        case INT32:
        case INT64:
            return "int";
        case DATE_TIME:
            return "date";
        case TIMESTAMP:
            return "timestamp";
        default:
            throw new IllegalStateException();
        }
    }
}
