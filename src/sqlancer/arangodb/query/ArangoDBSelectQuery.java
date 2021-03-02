package sqlancer.arangodb.query;

import java.io.IOException;
import java.util.List;

import com.arangodb.ArangoCursor;
import com.arangodb.entity.BaseDocument;

import sqlancer.GlobalState;
import sqlancer.arangodb.ArangoDBConnection;
import sqlancer.arangodb.ArangoDBQueryAdapter;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLancerResultSet;

public class ArangoDBSelectQuery extends ArangoDBQueryAdapter {

    private final String query;

    private List<BaseDocument> resultSet;

    public ArangoDBSelectQuery(String query) {
        this.query = query;
    }

    @Override
    public boolean couldAffectSchema() {
        return false;
    }

    @Override
    public <G extends GlobalState<?, ?, ArangoDBConnection>> boolean execute(G globalState, String... fills)
            throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public ExpectedErrors getExpectedErrors() {
        return new ExpectedErrors();
    }

    @Override
    public String getLogString() {
        return "db._query(\"" + query + "\")";
    }

    @Override
    public <G extends GlobalState<?, ?, ArangoDBConnection>> SQLancerResultSet executeAndGet(G globalState,
            String... fills) throws Exception {
        if (globalState.getOptions().logEachSelect()) {
            globalState.getLogger().writeCurrent(this.getLogString());
            try {
                globalState.getLogger().getCurrentFileWriter().flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        ArangoCursor<BaseDocument> cursor = globalState.getConnection().getDatabase().query(query, BaseDocument.class);
        resultSet = cursor.asListRemaining();
        return null;
    }

    public List<BaseDocument> getResultSet() {
        return resultSet;
    }
}
