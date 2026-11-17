package sqlancer.risingwave;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

public class TestRisingWaveConnection {
    private static final class Fixture {
        private final List<String> sql = new ArrayList<>();
        private final RisingWaveOptions options = new RisingWaveOptions();
        private final RisingWaveConnection connection;
        private boolean closed;
        private boolean closeOnCancel;
        private boolean failFlush;
        private boolean failReads;
        private int drops;
        private int queryTimeout;

        private Fixture() {
            options.statementTimeoutSeconds = 2;
            connection = new RisingWaveConnection((Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(),
                    new Class<?>[] { Connection.class }, (proxy, method, args) -> {
                        switch (method.getName()) {
                        case "createStatement":
                        case "prepareStatement":
                            if (closed) {
                                throw new SQLException("Connection closed", "08003");
                            }
                            return statement(args == null || args.length == 0 ? null : (String) args[0]);
                        case "isClosed":
                            return closed;
                        case "close":
                            closed = true;
                            return null;
                        default:
                            throw new UnsupportedOperationException(method.getName());
                        }
                    }), "owned", options, () -> drops++);
        }

        private Statement statement(String prepared) {
            Class<?> api = prepared == null ? Statement.class : PreparedStatement.class;
            return (Statement) Proxy.newProxyInstance(Statement.class.getClassLoader(), new Class<?>[] { api },
                    (proxy, method, args) -> {
                        switch (method.getName()) {
                        case "setQueryTimeout":
                            queryTimeout = (int) args[0];
                            return null;
                        case "close":
                            return null;
                        case "execute":
                            String query = args == null || args.length == 0 ? prepared : (String) args[0];
                            sql.add(query);
                            if (query.equals("SELECT slow")) {
                                closed = closeOnCancel;
                                throw new SQLException("Receive shutdown msg: Cancel", "XX000");
                            }
                            if (query.equals("FLUSH") && failFlush) {
                                throw new SQLTimeoutException("flush cancelled", "57014");
                            }
                            if (failReads && query.startsWith("SELECT")) {
                                throw new SQLException("Connection lost", "08006");
                            }
                            return true;
                        default:
                            throw new UnsupportedOperationException(method.getName());
                        }
                    });
        }
    }

    @Test
    public void cancellationSkipsWithoutProbingForBothStatementTypes() throws Exception {
        for (boolean prepared : List.of(false, true)) {
            Fixture fixture = new Fixture();
            try (Statement statement = prepared ? fixture.connection.prepareStatement("SELECT slow")
                    : fixture.connection.createStatement()) {
                SQLTimeoutException timeout = assertThrows(SQLTimeoutException.class, () -> {
                    if (prepared) {
                        ((PreparedStatement) statement).execute();
                    } else {
                        statement.execute("SELECT slow");
                    }
                });
                assertTrue(RisingWaveErrors.expressionErrors().errorIsExpected(timeout.getMessage()));
                assertTrue(fixture.connection.isUsable());
                assertEquals(List.of("SELECT slow"), fixture.sql);
                assertEquals(2, fixture.queryTimeout);
            }
            try (Statement next = fixture.connection.createStatement()) {
                assertTrue(next.execute("SELECT k FROM t"));
            }
            assertEquals(List.of("SELECT slow", "SELECT k FROM t"), fixture.sql);
            fixture.connection.close();
        }
    }

    @Test
    public void closedCancellationAndLaterReadFailuresAbort() throws Exception {
        Fixture closed = new Fixture();
        closed.closeOnCancel = true;
        try (Statement statement = closed.connection.createStatement()) {
            SQLException error = assertThrows(SQLException.class, () -> statement.execute("SELECT slow"));
            assertEquals("08006", error.getSQLState());
            assertNull(error.getCause());
            assertFalse(RisingWaveErrors.expressionErrors().errorIsExpected(error.getMessage()));
            assertFalse(closed.connection.isUsable());
            assertThrows(SQLException.class, closed.connection::createStatement);
        }
        closed.connection.close();
        Fixture later = new Fixture();
        try (Statement statement = later.connection.createStatement()) {
            assertThrows(SQLTimeoutException.class, () -> statement.execute("SELECT slow"));
            later.failReads = true;
            SQLException error = assertThrows(SQLException.class, () -> statement.execute("SELECT k FROM t"));
            assertFalse(RisingWaveErrors.expressionErrors().errorIsExpected(error.getMessage()));
            assertFalse(later.connection.isUsable());
        }
        later.connection.close();
    }

    @Test
    public void readsFlushPendingDmlForBothStatementTypes() throws Exception {
        Fixture fixture = new Fixture();
        try (Statement statement = fixture.connection.createStatement()) {
            statement.execute("INSERT INTO t VALUES (1)");
            statement.execute("SELECT k FROM t");
            statement.execute("SELECT k FROM t");
        }
        try (PreparedStatement write = (PreparedStatement) fixture.connection.prepareStatement("UPDATE t SET k=2");
                PreparedStatement read = (PreparedStatement) fixture.connection
                        .prepareStatement("WITH q AS (SELECT k FROM t) SELECT * FROM q")) {
            write.execute();
            read.execute();
        }
        assertEquals(List.of("INSERT INTO t VALUES (1)", "FLUSH", "SELECT k FROM t", "SELECT k FROM t",
                "UPDATE t SET k=2", "FLUSH", "WITH q AS (SELECT k FROM t) SELECT * FROM q"), fixture.sql);
        fixture.connection.close();
    }

    @Test
    public void failedFlushCannotBecomeAnExpectedSkip() throws Exception {
        Fixture fixture = new Fixture();
        fixture.failFlush = true;
        try (Statement statement = fixture.connection.createStatement()) {
            statement.execute("DELETE FROM t");
            SQLException error = assertThrows(SQLException.class, () -> statement.execute("SELECT k FROM t"));
            assertEquals("08006", error.getSQLState());
            assertFalse(RisingWaveErrors.expressionErrors().errorIsExpected(error.getMessage()));
            assertFalse(fixture.connection.isUsable());
            assertEquals(List.of("DELETE FROM t", "FLUSH"), fixture.sql);
        }
        fixture.connection.close();
    }

    @Test
    public void cancelledRunsKeepTheExistingRetentionPolicy() throws Exception {
        for (RisingWaveOptions.Retention retention : RisingWaveOptions.Retention.values()) {
            Fixture fixture = new Fixture();
            fixture.options.retention = retention;
            fixture.connection.beginRun();
            try (Statement statement = fixture.connection.createStatement()) {
                assertThrows(SQLTimeoutException.class, () -> statement.execute("SELECT slow"));
            }
            fixture.connection.close();
            assertEquals(0, fixture.drops);
            fixture.connection.finish(false);
            fixture.connection.close();
            boolean retained = retention != RisingWaveOptions.Retention.NONE;
            assertEquals(retained, fixture.connection.shouldRetainOwnedDatabase());
            assertEquals(retained ? 0 : 1, fixture.drops);
            Fixture candidate = new Fixture();
            candidate.options.retention = retention;
            candidate.connection.close();
            candidate.connection.close();
            assertEquals(1, candidate.drops);
        }
    }
}
