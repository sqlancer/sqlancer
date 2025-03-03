package sqlancer.postgres.gen;

import jnr.ffi.Struct;
import sqlancer.Randomly;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresProvider;
import sqlancer.postgres.PostgresProvider.Action;

public class PostgresExplainGenerator {

    private PostgresExplainGenerator(){

    }

    public static SQLQueryAdapter explain(PostgresGlobalState globalState) throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("EXPLAIN ");
        Action action;

        do {
            action = Randomly.fromOptions(PostgresProvider.Action.values());
        } while (action == Action.EXPLAIN);
        SQLQueryAdapter query = action.getQuery(globalState);
        sb.append(query);
        return new SQLQueryAdapter(sb.toString(),query.getExpectedErrors());
    }

    public static String explain(String selectStr) throws Exception{
        StringBuilder sb = new StringBuilder();
        sb.append("EXPLAIN ");
        sb.append(selectStr);
        return sb.toString();
    }

}
