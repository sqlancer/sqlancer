package sqlancer.sqlite3.gen;

import sqlancer.Randomly;
import sqlancer.common.query.Query;
import sqlancer.common.query.QueryAdapter;
import sqlancer.sqlite3.SQLite3Provider;
import sqlancer.sqlite3.SQLite3Provider.Action;
import sqlancer.sqlite3.SQLite3Provider.SQLite3GlobalState;

public final class SQLite3ExplainGenerator {

    private SQLite3ExplainGenerator() {
    }

    public static Query explain(SQLite3GlobalState globalState) throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("EXPLAIN ");
        if (Randomly.getBoolean()) {
            sb.append("QUERY PLAN ");
        }
        Action action;
        do {
            action = Randomly.fromOptions(SQLite3Provider.Action.values());
        } while (action == Action.EXPLAIN);
        Query query = action.getQuery(globalState);
        sb.append(query);
        return new QueryAdapter(sb.toString(), query.getExpectedErrors());
    }

}
