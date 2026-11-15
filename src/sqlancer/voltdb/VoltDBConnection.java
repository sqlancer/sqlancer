package sqlancer.voltdb;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Pattern;

import sqlancer.IgnoreMeException;
import sqlancer.SQLConnection;

/**
 * The provider's JDBC connection. Besides exposing metadata, which is how the provider reads a database's tables, it
 * works around two things that would otherwise fail runs with several threads:
 *
 * <ul>
 * <li>Statements VoltDB rejects because of a schema change on another thread are retried (see
 * {@link VoltDBErrors#isRetryable}). VoltDB rejects them before running them, so the retry is safe.</li>
 * <li>close() is idempotent. SQLancer closes each connection twice, and the VoltDB driver treats every close() as one
 * user leaving the client that all connections to the same URL share, so a second close() shuts that client down under
 * the other threads (ENG-30489).</li>
 * <li>getString() returns null for a NULL value. The VoltDB driver returns the type's NULL sentinel instead, such as
 * -2147483648 for an INTEGER, or throws for a DECIMAL, BOOLEAN or TIMESTAMP (ENG-4643).</li>
 * <li>While nodes are killed and rejoined during the run (--node-failures), a statement or metadata call that fails
 * because of a node failure (see {@link VoltDBErrors#isNodeFailure}) throws IgnoreMeException, which skips the
 * statement, the check or the database. Such a statement may have committed, so it is not retried.</li>
 * </ul>
 */
public class VoltDBConnection extends SQLConnection {

    // a statement the JDBC driver takes for a query (ENG-30603, see adHocQuery)
    private static final Pattern SELECT_PREAMBLE = Pattern.compile("^\\s*select\\s+.*",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    // ENG-30229: an OPS request (@SystemInformation, @SystemCatalog, @Statistics) sent while a host is joining or
    // rejoining waits 60 s and fails ("OPS request hit sixty second timeout"), and while it waits every other one is
    // refused ("Too many pending stat requests"). The driver's getMetaData() and DatabaseMetaData calls are such
    // requests; they are retried for up to OPS_RETRY_MILLIS, which covers an elastic join (run-sqlancer.sh
    // --resize-interval) or a rejoin.
    private static final long OPS_RETRY_MILLIS = 150_000;

    // ENG-30622: a catalog update that drops a table while an elastic remove streams its index snapshot fails the
    // snapshot ("No table streamer of Type TABLE_STREAM_ELASTIC_INDEX"), and the remove then waits forever at "Building
    // index for data migration". run-sqlancer.sh creates the --ddl-hold file around each shrink; while it exists, DDL
    // waits, for up to DDL_HOLD_MILLIS.
    private static final Pattern DDL = Pattern.compile("^\\s*(create|drop|alter|partition)\\b.*",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    private static final long DDL_HOLD_MILLIS = 900_000;

    // a schema change, which --xdcr runs on both clusters: DR TABLE too, which the mirror does not get
    private static final Pattern PEER_DDL = Pattern.compile("^\\s*(create|drop|alter|partition|dr)\\b.*",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    private final Connection connection;
    private final long retryMillis;
    private final boolean nodeFailures;
    private boolean closed;
    // the DIFF oracle's second cluster, running another VoltDB version (--mirror), or null; every statement that
    // changes the database is repeated on it, and the database is marked diverged when the two disagree on whether it
    // succeeded
    private VoltDBConnection mirror;
    private volatile boolean diverged;
    // whether a node failure or crash interrupted a call since the DURABLE oracle last compared the database
    private volatile boolean failureSeen;
    // with --xdcr, the other cluster of the XDCR pair, or null: DDL runs on both (XDCR replicates rows, not the
    // schema),
    // and the database is marked split when the peer refuses a change this cluster made
    private VoltDBConnection peer;
    private volatile boolean peerSplit;
    // the --ddl-hold file, or null
    private Path ddlHold;

    public VoltDBConnection(Connection connection, int retrySeconds, boolean nodeFailures) {
        super(connection);
        this.connection = connection;
        this.retryMillis = retrySeconds * 1000L;
        this.nodeFailures = nodeFailures;
    }

    @Override
    public Statement createStatement() throws SQLException {
        return retrying(connection.createStatement(), Statement.class);
    }

    @Override
    public Statement prepareStatement(String sql) throws SQLException {
        return retrying(connection.prepareStatement(sql), PreparedStatement.class);
    }

    // a stored procedure call, {call name(?, ...)}, retried as statements are
    public CallableStatement prepareCall(String sql) throws SQLException {
        return retrying(connection.prepareCall(sql), CallableStatement.class);
    }

    @Override
    public synchronized void close() throws SQLException {
        if (!closed) {
            closed = true;
            super.close();
            if (mirror != null) {
                mirror.close();
            }
        }
    }

    public void setMirror(VoltDBConnection mirror) {
        this.mirror = mirror;
    }

    public void setDdlHold(Path ddlHold) {
        this.ddlHold = ddlHold;
    }

    // waits while the --ddl-hold file exists, if sql is DDL
    private void holdDdl(Object... args) throws InterruptedException {
        if (ddlHold == null || args == null || args.length != 1 || !(args[0] instanceof String)
                || !DDL.matcher((String) args[0]).matches()) {
            return;
        }
        long deadline = System.currentTimeMillis() + DDL_HOLD_MILLIS;
        while (Files.exists(ddlHold) && System.currentTimeMillis() < deadline) {
            Thread.sleep(200);
        }
    }

    public void setPeer(VoltDBConnection peer) {
        this.peer = peer;
    }

    public VoltDBConnection getPeer() {
        return peer;
    }

    // whether a schema change succeeded here and failed on the XDCR peer, after which their tables may differ
    public boolean isPeerSplit() {
        return peerSplit;
    }

    // repeats a schema change that succeeded here on the XDCR peer
    private void peerDdl(Method method, Object... args) {
        if (peer == null || !isWrite(method, args) || !PEER_DDL.matcher((String) args[0]).matches()) {
            return;
        }
        try (Statement st = peer.createStatement()) {
            st.execute((String) args[0]);
        } catch (SQLException | IgnoreMeException e) {
            peerSplit = true;
        }
    }

    public VoltDBConnection getMirror() {
        return mirror;
    }

    // whether a statement that changes the database succeeded on one cluster and failed on the other, after which the
    // two databases may hold different rows
    public boolean isDiverged() {
        return diverged;
    }

    public boolean isFailureSeen() {
        return failureSeen;
    }

    public void clearFailureSeen() {
        failureSeen = false;
    }

    // whether a call is a statement that changes the database: execute or executeUpdate of anything but a query
    private static boolean isWrite(Method method, Object... args) {
        if (args == null || args.length != 1 || !(args[0] instanceof String)
                || !method.getName().equals("execute") && !method.getName().equals("executeUpdate")) {
            return false;
        }
        String sql = ((String) args[0]).trim();
        return !SELECT_PREAMBLE.matcher(sql).matches() && !sql.startsWith("(")
                && !sql.regionMatches(true, 0, "WITH", 0, 4);
    }

    // repeats a statement that changes the database on the mirror, and records whether the two disagreed
    private void mirrorWrite(Method method, Object[] args, boolean succeeded) {
        if (mirror == null || !isWrite(method, args)) {
            return;
        }
        String sql = (String) args[0];
        // XDCR's DR TABLE: the mirror has no DR
        if (sql.trim().regionMatches(true, 0, "DR ", 0, 3)) {
            return;
        }
        boolean mirrored;
        try (Statement st = mirror.createStatement()) {
            st.execute(sql);
            mirrored = true;
        } catch (SQLException | IgnoreMeException e) {
            mirrored = false;
        }
        if (mirrored != succeeded) {
            diverged = true;
        }
    }

    private static boolean isOpsTimeout(Throwable t) {
        String m = t instanceof SQLException ? t.getMessage() : null;
        return m != null
                && (m.contains("OPS request hit sixty second timeout") || m.contains("Too many pending stat requests"));
    }

    private interface MetaDataCall<T> {
        T call() throws SQLException, InvocationTargetException, IllegalAccessException;
    }

    private <T> T opsRetrying(MetaDataCall<T> call) throws Throwable {
        long deadline = System.currentTimeMillis() + OPS_RETRY_MILLIS;
        while (true) {
            try {
                return call.call();
            } catch (SQLException | InvocationTargetException e) {
                Throwable cause = e instanceof InvocationTargetException ? e.getCause() : e;
                if (isOpsTimeout(cause) && System.currentTimeMillis() < deadline) {
                    Thread.sleep(1000);
                    continue;
                }
                throw skippedIfNodeFailure(cause);
            }
        }
    }

    public DatabaseMetaData getMetaData() throws SQLException {
        DatabaseMetaData md;
        try {
            // the driver reads the cluster's system information here
            md = opsRetrying(connection::getMetaData);
        } catch (Throwable t) {
            if (t instanceof SQLException) {
                throw (SQLException) t;
            }
            if (t instanceof RuntimeException) {
                throw (RuntimeException) t;
            }
            throw new SQLException(t);
        }
        return (DatabaseMetaData) Proxy.newProxyInstance(DatabaseMetaData.class.getClassLoader(),
                new Class<?>[] { DatabaseMetaData.class },
                (proxy, method, args) -> opsRetrying(() -> method.invoke(md, args)));
    }

    // an error a node failure caused, as IgnoreMeException while --node-failures is on, otherwise the error itself
    private Throwable skippedIfNodeFailure(Throwable t) {
        if (nodeFailures && t instanceof SQLException && VoltDBErrors.isNodeFailure((SQLException) t)) {
            return new IgnoreMeException();
        }
        return t;
    }

    // wraps a statement so that its execute* methods retry while VoltDB rejects them as retryable
    private <T extends Statement> T retrying(T statement, Class<T> type) {
        return type
                .cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] { type }, (proxy, method, args) -> {
                    if (method.getName().startsWith("execute")) {
                        holdDdl(args);
                    }
                    long deadline = System.currentTimeMillis() + retryMillis;
                    while (true) {
                        try {
                            Object result = isNonSelectQuery(method, args) ? adHocQuery((String) args[0])
                                    : method.invoke(statement, args);
                            mirrorWrite(method, args, true);
                            peerDdl(method, args);
                            return result instanceof ResultSet ? nullReading((ResultSet) result) : result;
                        } catch (InvocationTargetException e) {
                            Throwable cause = e.getCause();
                            if (method.getName().startsWith("execute") && cause instanceof SQLException
                                    && VoltDBErrors.isRetryable((SQLException) cause)
                                    && System.currentTimeMillis() < deadline) {
                                Thread.sleep(ThreadLocalRandom.current().nextLong(20, 100));
                                continue;
                            }
                            Throwable thrown = skippedIfNodeFailure(cause);
                            if (thrown instanceof IgnoreMeException) {
                                failureSeen = true;
                                // a node failure: a change may or may not have committed; a query changed nothing
                                diverged = mirror != null && isWrite(method, args) || diverged;
                            } else {
                                mirrorWrite(method, args, false);
                            }
                            throw thrown;
                        }
                    }
                }));
    }

    // ENG-30603: the JDBC driver takes a statement for a query only if it starts with SELECT, and executeQuery()
    // rejects any other ("Cannot submit statement in current context"), so a query that starts with WITH or with a
    // parenthesized set operation goes through @AdHoc as a stored procedure call instead, which returns its rows
    private static boolean isNonSelectQuery(Method method, Object... args) {
        return method.getName().equals("executeQuery") && args != null && args.length == 1 && args[0] instanceof String
                && !SELECT_PREAMBLE.matcher((String) args[0]).matches();
    }

    // its errors as the statement's would be, wrapped in InvocationTargetException
    private ResultSet adHocQuery(String sql) throws InvocationTargetException {
        try {
            CallableStatement call = connection.prepareCall("{call @AdHoc(?)}");
            call.setString(1, sql);
            return call.executeQuery();
        } catch (SQLException e) {
            throw new InvocationTargetException(e);
        }
    }

    // wraps a result set so that getString() and getObject() return null for a NULL value (ENG-4643); getObject()
    // returns the type's sentinel, but wasNull() is right after it
    private static ResultSet nullReading(ResultSet rs) {
        return (ResultSet) Proxy.newProxyInstance(ResultSet.class.getClassLoader(), new Class<?>[] { ResultSet.class },
                (proxy, method, args) -> {
                    try {
                        if ("getString".equals(method.getName())
                                || "getObject".equals(method.getName()) && args.length == 1) {
                            Object value;
                            if (args[0] instanceof Integer) {
                                value = rs.getObject((Integer) args[0]);
                            } else {
                                value = rs.getObject((String) args[0]);
                            }
                            if (rs.wasNull()) {
                                return null;
                            }
                            if ("getObject".equals(method.getName())) {
                                return value;
                            }
                        }
                        return method.invoke(rs, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }

}
