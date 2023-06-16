package sqlancer.reducer.VirtualDB;

import sqlancer.SQLConnection;
import sqlancer.SQLGlobalState;
import sqlancer.common.query.Query;

import java.util.List;
import java.util.function.Function;

@SuppressWarnings("all")
public class VirtualDBGlobalState extends SQLGlobalState<VirtualDBOptions, VirtualDBSchema> {

    private SQLConnection virtualConn = new SQLConnection(null);
    private StringBuilder queriesStringBuilder = new StringBuilder();
    private Function<List<Query<?>>, Boolean> bugInducingCondition = null;

    public Function<List<Query<?>>, Boolean> getBugInducingCondition() {
        return bugInducingCondition;
    }

    public void setBugInducingCondition(Function<List<Query<?>>, Boolean> condition) {
        bugInducingCondition = (condition);
    }

    @Override
    protected VirtualDBSchema readSchema() throws Exception {
        return null;
    }

    @Override
    public SQLConnection getConnection() {
        // It's a fake engine, so the connection would not be available :)
        return virtualConn;
    }

    @Override
    public void setConnection(SQLConnection con) {
        // A fake connection could also not be closed.
        // So nothing would be done here.
        // And reset the query String (Seems needless)
        // queriesStringBuilder = new StringBuilder();
    }

    // public String getCurrentQueriesString() {
    // return queriesStringBuilder.toString();
    // }

    @Override
    public boolean executeStatement(Query<SQLConnection> q, String... fills) throws Exception {
        if (queriesStringBuilder.length() != 0) {
            queriesStringBuilder.append("\n");
        }
        queriesStringBuilder.append(q.getQueryString());
        return true;
    }
}
