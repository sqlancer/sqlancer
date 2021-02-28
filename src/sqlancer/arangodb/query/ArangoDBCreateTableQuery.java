package sqlancer.arangodb.query;

import sqlancer.GlobalState;
import sqlancer.Main;
import sqlancer.arangodb.ArangoDBConnection;
import sqlancer.arangodb.ArangoDBQueryAdapter;
import sqlancer.common.query.ExpectedErrors;

public class ArangoDBCreateTableQuery extends ArangoDBQueryAdapter {

    private final String tableName;

    public ArangoDBCreateTableQuery(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public boolean couldAffectSchema() {
        return true;
    }

    @Override
    public <G extends GlobalState<?, ?, ArangoDBConnection>> boolean execute(G globalState, String... fills)
            throws Exception {
        try {
            globalState.getConnection().getDatabase().createCollection(tableName);
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
        return "db._create(\"" + tableName + "\")";
    }
}
