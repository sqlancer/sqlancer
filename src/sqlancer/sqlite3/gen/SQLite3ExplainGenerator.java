package sqlancer.sqlite3.gen;

import sqlancer.Randomly;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.sqlite3.SQLite3GlobalState;
import sqlancer.sqlite3.SQLite3Provider;
import sqlancer.sqlite3.SQLite3Provider.Action;

public final class SQLite3ExplainGenerator {

    private SQLite3ExplainGenerator() {
    }

    public static SQLQueryAdapter explain(SQLite3GlobalState globalState) throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("EXPLAIN ");
        if (Randomly.getBoolean()) {
            sb.append("QUERY PLAN ");
        }
        Action action;
        do {
            action = Randomly.fromOptions(SQLite3Provider.Action.values());
        } while (action == Action.EXPLAIN);
        SQLQueryAdapter query = action.getQuery(globalState);
        sb.append(query);
        return new SQLQueryAdapter(sb.toString(), query.getExpectedErrors());
    }

}
