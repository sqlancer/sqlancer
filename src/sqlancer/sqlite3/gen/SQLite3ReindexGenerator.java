package sqlancer.sqlite3.gen;

import java.util.ArrayList;
import java.util.List;

import sqlancer.Query;
import sqlancer.QueryAdapter;
import sqlancer.Randomly;
import sqlancer.sqlite3.SQLite3Provider.SQLite3GlobalState;
import sqlancer.sqlite3.schema.SQLite3Schema;

/**
 * @see https://www.sqlite.org/lang_reindex.html
 */
public class SQLite3ReindexGenerator {

    private enum Target {
        TABLE, INDEX, COLLATION_NAME
    }

    public static Query executeReindex(SQLite3GlobalState globalState) {
        SQLite3Schema s = globalState.getSchema();
        StringBuilder sb = new StringBuilder("REINDEX");
        List<String> errors = new ArrayList<>();
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
        return new QueryAdapter(sb.toString(), errors, true);
    }
}
