package sqlancer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.Query;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.sqlite3.SQLite3Provider;

public class TestStateToReproduce {

    @TempDir
    Path tempDir;

    @Test
    public void testBasicFields() throws IOException {
        SQLite3Provider provider = new SQLite3Provider();
        StateToReproduce state = new StateToReproduce("test_db", provider);
        state.databaseVersion = "3.36.0";
        state.seedValue = 12345L;
        state.exception = "Test exception message";

        Path file = tempDir.resolve("test_basic.ser");
        state.serialize(file);
        StateToReproduce result = StateToReproduce.deserialize(file);

        assertEquals(state.getDatabaseName(), result.getDatabaseName());
        assertEquals(state.getDatabaseVersion(), result.getDatabaseVersion());
        assertEquals(state.getSeedValue(), result.getSeedValue());
        assertEquals(state.getException(), result.getException());
    }

    @Test
    public void testStatements() throws IOException {
        SQLite3Provider provider = new SQLite3Provider();
        StateToReproduce state = new StateToReproduce("test_statements", provider);
        List<Query<?>> statements = new ArrayList<>();

        ExpectedErrors errors1 = new ExpectedErrors();
        errors1.add("syntax error");
        errors1.add("table already exists");
        statements.add(new SQLQueryAdapter("CREATE TABLE test (id INTEGER);", errors1));

        ExpectedErrors errors2 = new ExpectedErrors();
        errors2.add("constraint failed");
        statements.add(new SQLQueryAdapter("INSERT INTO test VALUES (1);", errors2));

        statements.add(new SQLQueryAdapter("SELECT * FROM test;", new ExpectedErrors()));
        state.setStatements(statements);

        Path file = tempDir.resolve("test_statements.ser");
        state.serialize(file);
        StateToReproduce result = StateToReproduce.deserialize(file);

        List<Query<?>> resultStatements = result.getStatements();
        assertEquals(3, resultStatements.size());

        Query<?> q1 = resultStatements.get(0);
        Query<?> q2 = resultStatements.get(1);
        Query<?> q3 = resultStatements.get(2);

        assertEquals("CREATE TABLE test (id INTEGER);", q1.getLogString());
        assertEquals("INSERT INTO test VALUES (1);", q2.getLogString());
        assertEquals("SELECT * FROM test;", q3.getLogString());

        ExpectedErrors e1 = q1.getExpectedErrors();
        ExpectedErrors e2 = q2.getExpectedErrors();
        ExpectedErrors e3 = q3.getExpectedErrors();

        assertTrue(e1.errorIsExpected("syntax error"));
        assertTrue(e1.errorIsExpected("table already exists"));
        assertFalse(e1.errorIsExpected("constraint failed"));

        assertFalse(e2.errorIsExpected("syntax error"));
        assertTrue(e2.errorIsExpected("constraint failed"));

        assertFalse(e3.errorIsExpected("syntax error"));
        assertFalse(e3.errorIsExpected("constraint failed"));
    }

    @Test
    public void testDatabaseProvider() throws IOException {
        SQLite3Provider provider = new SQLite3Provider();
        StateToReproduce state = new StateToReproduce("test_provider", provider);
        state.logStatement("CREATE TABLE test (id INTEGER);");

        Path file = tempDir.resolve("test_provider.ser");
        state.serialize(file);
        StateToReproduce result = StateToReproduce.deserialize(file);

        // Verify databaseProvider is correctly deserialized
        assertEquals("sqlite3", result.getDatabaseProvider().getDBMSName());

        // Verify databaseProvider functionality by testing logStatement
        result.logStatement("INSERT INTO test VALUES (1);");
        assertEquals(2, result.getStatements().size());
        assertEquals("INSERT INTO test VALUES (1);", result.getStatements().get(1).getLogString());
    }
}