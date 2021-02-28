package sqlancer.arangodb.query;

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
        // TODO Patrick
        return "";
    }
}
