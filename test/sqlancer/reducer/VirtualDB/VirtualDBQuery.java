package sqlancer.reducer.VirtualDB;

import sqlancer.GlobalState;
import sqlancer.SQLConnection;
import sqlancer.common.query.SQLQueryAdapter;

import java.sql.SQLException;

public class VirtualDBQuery extends SQLQueryAdapter {

    public VirtualDBQuery(String query) {
        super(query);
    }

    public VirtualDBQuery(String query, boolean couldAffectSchema) {
        super(query, couldAffectSchema);
    }

    @Override
    public <G extends GlobalState<?, ?, SQLConnection>> boolean execute(G globalState, String... fills)
            throws SQLException {
        try {
            return globalState.executeStatement(this, fills);
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }
}
