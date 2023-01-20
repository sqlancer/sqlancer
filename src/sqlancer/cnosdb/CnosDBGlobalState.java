package sqlancer.cnosdb;

import sqlancer.ExecutionTimer;
import sqlancer.GlobalState;
import sqlancer.cnosdb.client.CnosDBConnection;
import sqlancer.common.query.Query;

public class CnosDBGlobalState extends GlobalState<CnosDBOptions, CnosDBSchema, CnosDBConnection> {

    @Override
    protected void executeEpilogue(Query<?> q, boolean success, ExecutionTimer timer) throws Exception {
        boolean logExecutionTime = getOptions().logExecutionTime();
        if (success && getOptions().printSucceedingStatements()) {
            System.out.println(q.getQueryString());
        }
        if (logExecutionTime) {
            getLogger().writeCurrent(" -- " + timer.end().asString());
        }
        if (q.couldAffectSchema()) {
            updateSchema();
        }
    }

    @Override
    public CnosDBSchema readSchema() throws Exception {
        return CnosDBSchema.fromConnection(getConnection());
    }
}
