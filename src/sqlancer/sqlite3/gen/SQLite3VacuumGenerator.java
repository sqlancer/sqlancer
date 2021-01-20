package sqlancer.sqlite3.gen;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.sqlite3.SQLite3GlobalState;

/**
 * @see <a href="https://www.sqlite.org/lang_vacuum.html">VACUUM</a>
 */
public final class SQLite3VacuumGenerator {

    private SQLite3VacuumGenerator() {
    }

    public static SQLQueryAdapter executeVacuum(SQLite3GlobalState globalState) {
        StringBuilder sb = new StringBuilder("VACUUM");
        if (Randomly.getBoolean()) {
            sb.append(" ");
            sb.append(Randomly.fromOptions("temp", "main"));
        }
        return new SQLQueryAdapter(sb.toString(), ExpectedErrors.from("cannot VACUUM from within a transaction",
                "cannot VACUUM - SQL statements in progress", "The database file is locked"));
    }

}
