package sqlancer.risingwave;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.util.Locale;

import sqlancer.SQLConnection;

/** Flushes pending writes before reads and manages cleanup of the test database. */
public final class RisingWaveConnection extends SQLConnection {
    private final Connection raw;
    private final String database;
    private final RisingWaveOptions options;
    private final Cleanup cleanup;
    private boolean closed;
    private boolean finalized;
    private boolean managedRun;
    private boolean managedLifecycle;
    private boolean failed;
    private boolean unusable;
    private boolean dirty;
    private SQLException cleanupFailure;
    private RisingWaveErrors.Category lastError;

    @FunctionalInterface
    public interface Cleanup {
        void drop() throws SQLException;
    }

    public RisingWaveConnection(Connection raw, String database, RisingWaveOptions options, Cleanup cleanup) {
        super(raw);
        this.raw = raw;
        this.database = database;
        this.options = options;
        this.cleanup = cleanup;
    }

    public String getDatabase() {
        return database;
    }

    public RisingWaveErrors.Category getLastError() {
        return lastError;
    }

    public void resetLastError() {
        lastError = null;
    }

    public boolean isUsable() {
        return !closed && !unusable;
    }

    public SQLException getCleanupFailure() {
        return cleanupFailure;
    }

    public boolean shouldRetainOwnedDatabase() {
        return managedLifecycle && (options.retention == RisingWaveOptions.Retention.ALL
                || failed && options.retention == RisingWaveOptions.Retention.FAILURES);
    }

    public void beginRun() {
        managedRun = true;
        managedLifecycle = true;
    }

    public void finish(boolean failure) {
        failed |= failure;
        managedRun = false;
        try {
            close();
        } catch (SQLException e) {
            reportCleanup(e);
        }
    }

    @Override
    public Statement createStatement() throws SQLException {
        if (unusable || closed) {
            throw new SQLException("RisingWave connection is unusable", "08006");
        }
        return guard(raw.createStatement(), Statement.class, null);
    }

    @Override
    public Statement prepareStatement(String sql) throws SQLException {
        if (unusable || closed) {
            throw new SQLException("RisingWave connection is unusable", "08006");
        }
        return guard(raw.prepareStatement(sql), java.sql.PreparedStatement.class, sql);
    }

    private Statement guard(Statement statement, Class<?> api, String preparedSql) throws SQLException {
        statement.setQueryTimeout(options.statementTimeoutSeconds);
        return (Statement) Proxy.newProxyInstance(Statement.class.getClassLoader(), new Class<?>[] { api },
                (proxy, method, arguments) -> {
                    // Prepared statements use the SQL passed to prepareStatement().
                    String sql = preparedSql;
                    if (arguments != null && arguments.length > 0 && arguments[0] instanceof String) {
                        sql = (String) arguments[0];
                    }
                    String command = sql == null ? "" : sql.trim().toUpperCase(Locale.ROOT);
                    // Writes may not be visible to queries until FLUSH.
                    // The reducer may remove the FLUSH issued during setup.
                    if (method.getName().startsWith("execute") && dirty
                            && (command.startsWith("SELECT") || command.startsWith("WITH"))) {
                        synchronizeWrites();
                    }
                    try {
                        Object result = method.invoke(statement, arguments);
                        if (method.getName().startsWith("execute")) {
                            if (command.startsWith("INSERT") || command.startsWith("UPDATE")
                                    || command.startsWith("DELETE")) {
                                dirty = true;
                            } else if (command.startsWith("FLUSH")) {
                                dirty = false;
                            }
                        }
                        return result;
                    } catch (InvocationTargetException invocation) {
                        Throwable cause = invocation.getCause();
                        if (cause instanceof SQLException) {
                            SQLException error = (SQLException) cause;
                            lastError = RisingWaveErrors.classify(error);
                            if (lastError == RisingWaveErrors.Category.TIMEOUT) {
                                failed |= managedRun;
                                // Abort the run if cancellation closed the connection.
                                if (raw.isClosed()) {
                                    unusable = true;
                                    SQLException closedConnection = new SQLException(
                                            "RisingWave connection closed after cancellation", "08006");
                                    closedConnection.addSuppressed(error);
                                    throw closedConnection;
                                }
                                throw new SQLTimeoutException(RisingWaveErrors.TIMEOUT_MARKER, "57014", error);
                            }
                            if (lastError == RisingWaveErrors.Category.UNEXPECTED) {
                                failed |= managedRun;
                                unusable |= error.getSQLState() != null && error.getSQLState().startsWith("08");
                            }
                        }
                        throw cause;
                    }
                });
    }

    private void synchronizeWrites() throws SQLException {
        try (Statement barrier = raw.createStatement()) {
            barrier.setQueryTimeout(options.statementTimeoutSeconds);
            barrier.execute("FLUSH");
            dirty = false;
        } catch (SQLException failure) {
            unusable = true;
            failed |= managedRun;
            SQLException synchronization = new SQLException("RisingWave write synchronization failed", "08006");
            synchronization.addSuppressed(failure);
            throw synchronization;
        }
    }

    @Override
    public void close() throws SQLException {
        if (!closed) {
            try {
                raw.close();
            } catch (SQLException closeFailure) {
                // Keep the database if closing its connection failed.
                failed = true;
                finalized = true;
                reportCleanup(closeFailure);
            } finally {
                closed = true;
            }
        }
        // Defer database cleanup until finish() knows whether the run failed.
        if (managedRun || finalized) {
            return;
        }
        finalized = true;
        if (shouldRetainOwnedDatabase()) {
            System.err.println("Retained RisingWave database: " + database);
            return;
        }
        try {
            cleanup.drop();
        } catch (SQLException e) {
            reportCleanup(e);
        }
    }

    private void reportCleanup(SQLException error) {
        cleanupFailure = error;
        System.err.println("RisingWave cleanup failed for " + database + ": " + error.getMessage());
    }
}
