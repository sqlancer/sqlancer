package sqlancer.sqlite3;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.sql.Connection;
import java.sql.DriverManager;

import org.junit.jupiter.api.Test;

import sqlancer.MainOptions;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.sqlite3.gen.ddl.SQLite3TableGenerator;

/**
 * Since SQLite 3.44.0 a CREATE TABLE whose generated columns depend on each other fails right away, where it used to
 * succeed and fail on first use. The table generator did not expect that error, so the 2026-10-05 trunk run lost a
 * thread to an AssertionError on the statement below.
 */
public class TestSQLite3GeneratedColumnLoop {

    private static final String LOOP = "CREATE TABLE IF NOT EXISTS t1 (c0 TEXT GENERATED ALWAYS AS (INSTR(((c2) NOT NULL),"
            + " (((NULL, c1, c1, c1, x''))<>((c1, c2, c2, c1, c2))))) UNIQUE ON CONFLICT REPLACE NOT NULL CHECK"
            + " ( (0.20277664448948018 IN (c0))) , c1 TEXT UNIQUE ON CONFLICT ABORT GENERATED ALWAYS AS"
            + " ((((((((((+ (c0)))AND((((c0)) BETWEEN ((c0)) AND ((c0))))))AND(CASE WHEN c0 THEN c2 WHEN c0 THEN c0"
            + " WHEN c2 THEN c2 ELSE c0 END)))OR(CAST(c2 AS INTEGER))))OR(((c0)-(c0))))) , c2 TEXT );";

    @Test
    public void aGeneratedColumnLoopAtCreateTableIsExpected() throws Exception {
        Class.forName("org.sqlite.JDBC");
        try (Connection connection = DriverManager.getConnection("jdbc:sqlite::memory:")) {
            SQLite3GlobalState state = new SQLite3GlobalState();
            state.setConnection(new SQLConnection(connection));
            state.setMainOptions(new MainOptions());
            state.setDbmsSpecificOptions(new SQLite3Options());
            state.setRandomly(new Randomly(1));
            state.updateSchema();

            SQLQueryAdapter generated = SQLite3TableGenerator.createTableStatement("t1", state);
            SQLQueryAdapter loop = new SQLQueryAdapter(LOOP, generated.getExpectedErrors(), true);
            assertDoesNotThrow(() -> assertFalse(loop.execute(state), "SQLite refuses the loop at CREATE TABLE"));
        }
    }
}
