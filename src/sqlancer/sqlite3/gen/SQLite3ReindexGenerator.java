package sqlancer.sqlite3.gen;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.sqlite3.SQLite3GlobalState;
import sqlancer.sqlite3.schema.SQLite3Schema;

/**
 * @see <a href="https://www.sqlite.org/lang_reindex.html">REINDEX</a>
 */
public final class SQLite3ReindexGenerator {

    private SQLite3ReindexGenerator() {
    }

    private enum Target {
        TABLE, INDEX, COLLATION_NAME
    }

    public static SQLQueryAdapter executeReindex(SQLite3GlobalState globalState) {
        SQLite3Schema s = globalState.getSchema();
        StringBuilder sb = new StringBuilder("REINDEX");
        ExpectedErrors errors = new ExpectedErrors();
        errors.add("The database file is locked");
        Target t = Randomly.fromOptions(Target.values());
        if (Randomly.getBoolean()) {
            sb.append(" ");
            switch (t) {
            case INDEX:
                sb.append(s.getRandomIndexOrBailout());
                // temp table
                errors.add("unable to identify the object to be reindexed");
                break;
            case COLLATION_NAME:
                sb.append(Randomly.fromOptions("BINARY", "NOCASE", "RTRIM"));
                break;
            case TABLE:
                sb.append(" ");
                sb.append(s.getRandomTableOrBailout(tab -> !tab.isTemp() && !tab.isView()).getName());
                break;
            default:
                throw new AssertionError(t);
            }
        }
        return new SQLQueryAdapter(sb.toString(), errors, true);
    }
}
