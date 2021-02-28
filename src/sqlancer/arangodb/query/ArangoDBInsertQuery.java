package sqlancer.arangodb.query;

import java.util.Map;

import com.arangodb.entity.BaseDocument;

import sqlancer.GlobalState;
import sqlancer.Main;
import sqlancer.arangodb.ArangoDBConnection;
import sqlancer.arangodb.ArangoDBQueryAdapter;
import sqlancer.arangodb.ArangoDBSchema;
import sqlancer.common.query.ExpectedErrors;

public class ArangoDBInsertQuery extends ArangoDBQueryAdapter {

    private final ArangoDBSchema.ArangoDBTable table;
    private final BaseDocument documentToBeInserted;

    public ArangoDBInsertQuery(ArangoDBSchema.ArangoDBTable table, BaseDocument documentToBeInserted) {
        this.table = table;
        this.documentToBeInserted = documentToBeInserted;
    }

    @Override
    public boolean couldAffectSchema() {
        return true;
    }

    @Override
    public <G extends GlobalState<?, ?, ArangoDBConnection>> boolean execute(G globalState, String... fills)
            throws Exception {
        try {
            globalState.getConnection().getDatabase().collection(table.getName()).insertDocument(documentToBeInserted);
            Main.nrSuccessfulActions.addAndGet(1);
            return true;
        } catch (Exception e) {
            Main.nrUnsuccessfulActions.addAndGet(1);
            throw e;
        }
    }

    @Override
    public ExpectedErrors getExpectedErrors() {
        return new ExpectedErrors();
    }

    @Override
    public String getLogString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("db._query(\"INSERT { ");
        String filler = "";
        for (Map.Entry<String, Object> stringObjectEntry : documentToBeInserted.getProperties().entrySet()) {
            stringBuilder.append(filler);
            filler = ", ";
            stringBuilder.append(stringObjectEntry.getKey()).append(": ");
            Object value = stringObjectEntry.getValue();
            if (value instanceof String) {
                stringBuilder.append("'").append(value).append("'");
            } else {
                stringBuilder.append(value);
            }
        }
        stringBuilder.append("} IN ").append(table.getName()).append("\")");
        return stringBuilder.toString();
    }
}
