package sqlancer.cnosdb.query;

import sqlancer.GlobalState;
import sqlancer.IgnoreMeException;
import sqlancer.cnosdb.client.CnosDBConnection;
import sqlancer.common.query.ExpectedErrors;

public class CnosDBOtherQuery extends CnosDBQueryAdapter {
    public CnosDBOtherQuery(String query, ExpectedErrors errors) {
        super(query, errors);
    }

    void setExpectedErrors(ExpectedErrors errors) {

    }

    @Override
    public boolean couldAffectSchema() {
        return true;
    }

    @Override
    public <G extends GlobalState<?, ?, CnosDBConnection>> boolean execute(G globalState, String... fills)
            throws Exception {
        try {
            globalState.getConnection().getClient().execute(query);
        } catch (Exception e) {
            if (this.errors.errorIsExpected(e.getMessage())) {
                throw new IgnoreMeException();
            }
        }
        return true;
    }
}
