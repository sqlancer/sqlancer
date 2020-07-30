package sqlancer.sqlite3.gen;

import java.util.Arrays;

import sqlancer.Query;
import sqlancer.QueryAdapter;
import sqlancer.Randomly;
import sqlancer.sqlite3.SQLite3Provider.SQLite3GlobalState;

/**
 * @see https://www.sqlite.org/lang_vacuum.html
 */
public final class SQLite3VacuumGenerator {

    private SQLite3VacuumGenerator() {
    }

    public static Query executeVacuum(SQLite3GlobalState globalState) {
        StringBuilder sb = new StringBuilder("VACUUM");
        if (Randomly.getBoolean()) {
            sb.append(" ");
            sb.append(Randomly.fromOptions("temp", "main"));
        }
        return new QueryAdapter(sb.toString(),
                Arrays.asList("cannot VACUUM from within a transaction", "cannot VACUUM - SQL statements in progress"));
    }

}
