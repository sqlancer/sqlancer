package sqlancer.voltdb.oracle;

import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.Reproducer;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.oracle.TestOracleUtils;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.voltdb.VoltDBErrors;
import sqlancer.voltdb.VoltDBProvider.VoltDBGlobalState;
import sqlancer.voltdb.VoltDBSchema.VoltDBTable;
import sqlancer.voltdb.ast.VoltDBExpression;
import sqlancer.voltdb.ast.VoltDBSelect;
import sqlancer.voltdb.gen.VoltDBDeleteGenerator;
import sqlancer.voltdb.gen.VoltDBExpressionGenerator;
import sqlancer.voltdb.gen.VoltDBInsertGenerator;
import sqlancer.voltdb.gen.VoltDBUpdateGenerator;

/**
 * Permissions (run-sqlancer.sh --security). The users reader (role sqlancer_read, SQLREAD) and writer (role
 * sqlancer_write, SQL) may do what their roles allow and nothing else, and a statement VoltDB refuses must change
 * nothing. PERMS runs, as one of the two users:
 * <ul>
 * <li>a query: allowed to both; its rows must equal the administrator's for the same query;</li>
 * <li>a change (INSERT, UPDATE, DELETE from the provider's generators): refused to reader, with a permission error, and
 * every table must then hold as many rows as before; for writer, a change that matches no row ({@code UPDATE t
 * SET c = c WHERE 1 = 0}, {@code DELETE FROM t WHERE 1 = 0}) must be allowed, as a real one would put this cluster out
 * of step with the mirror;</li>
 * <li>DDL ({@code CREATE INDEX}, {@code DROP TABLE}): refused to both, and nothing may change;</li>
 * <li>a stored procedure the oracle creates with {@code ALLOW sqlancer_read}: allowed to reader, refused to writer (SQL
 * does not include ALLPROC).</li>
 * </ul>
 * The database's thread is its only writer, so the row counts cannot change for another reason. Without --security,
 * PERMS skips every check. At exit it prints how many checks it made of each kind.
 */
public class VoltDBPermissionOracle implements TestOracle<VoltDBGlobalState> {

    private enum User {
        READER("reader"), WRITER("writer");

        final String name;

        User(String name) {
            this.name = name;
        }
    }

    private static final Pattern REFUSED = Pattern
            .compile("(?is).*(does not have .*permission|lacks permission|permission denied|not authorized).*");
    // the users' connections, one each, shared by every thread: the JDBC driver shares its client per URL and user
    private static final Connection[] CONNECTIONS = new Connection[User.values().length];
    private static final AtomicLong ALLOWED = new AtomicLong();
    private static final AtomicLong REFUSED_CHECKS = new AtomicLong();

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> System.out.println(
                "PERMS checked: " + ALLOWED + " allowed statements, " + REFUSED_CHECKS + " refused statements")));
    }

    private final VoltDBGlobalState state;
    private final ExpectedErrors errors;
    private String lastQuery;

    public VoltDBPermissionOracle(VoltDBGlobalState state, ExpectedErrors errors) {
        this.state = state;
        this.errors = errors;
    }

    @Override
    public void check() throws Exception {
        if (!state.getDbmsSpecificOptions().security) {
            throw new IgnoreMeException();
        }
        User user = Randomly.fromOptions(User.values());
        Connection connection = connection(state, user);
        VoltDBTable table = state.getSchema().getRandomTable(t -> !t.isView());
        // a procedure check (3) costs two catalog updates, so one check in eight
        switch (Randomly.fromOptions(0, 0, 1, 1, 2, 2, 0, 3)) {
        case 0:
            query(user, connection);
            break;
        case 1:
            if (user == User.READER) {
                String change = Randomly.fromOptions(VoltDBInsertGenerator.getQuery(state, table),
                        VoltDBUpdateGenerator.getQuery(state, table), VoltDBDeleteGenerator.getQuery(state, table))
                        .getQueryString();
                refused(user, connection, change);
            } else {
                String column = Randomly.fromList(table.getColumns()).getName();
                allowed(user, connection,
                        Randomly.fromOptions(
                                "UPDATE " + table.getName() + " SET " + column + " = " + column + " WHERE 1 = 0",
                                "DELETE FROM " + table.getName() + " WHERE 1 = 0"));
            }
            break;
        case 2:
            String ddl;
            if (state.getDbmsSpecificOptions().avoidKnownBugs) {
                // ENG-30627: VoltDB compiles a user's DDL before it checks the user's permission, so DDL whose new
                // catalog does not compile (a DROP TABLE that a procedure reads) gets the compile error; a new table
                // always compiles
                ddl = "CREATE TABLE " + state.getDatabaseName().toUpperCase() + "_PERMS_T (a INTEGER)";
            } else {
                String column = Randomly.fromList(table.getColumns()).getName();
                ddl = Randomly.fromOptions(
                        "CREATE INDEX " + state.getFreeIndexName() + " ON " + table.getName() + " (" + column + ")",
                        "DROP TABLE " + table.getName() + " IF EXISTS CASCADE");
            }
            refused(user, connection, ddl);
            break;
        default:
            procedure(user, connection, table);
            break;
        }
    }

    // a query: the user's rows must be the administrator's
    private void query(User user, Connection connection) throws SQLException {
        VoltDBExpressionGenerator gen = new VoltDBExpressionGenerator(state);
        gen.setTablesAndColumns(TestOracleUtils.getRandomTableNonEmptyTables(state.getSchema()));
        VoltDBSelect select = gen.generateSelect();
        select.setFromList(gen.getTableRefs());
        select.setJoinClauses(gen.getRandomJoinClauses());
        List<VoltDBExpression> fetched = gen.generateFetchColumns();
        select.setFetchColumns(fetched);
        select.setWhereClause(gen.generateBooleanExpression());
        String sql = select.asString();
        lastQuery = sql;
        List<String> admin = rows(() -> state.getConnection().createStatement(), sql, fetched.size(), null);
        List<String> theirs = rows(connection::createStatement, sql, fetched.size(), user);
        ALLOWED.incrementAndGet();
        if (!admin.equals(theirs)) {
            throw new AssertionError("the " + user.name + " user got other rows than the administrator for " + sql
                    + "; only the administrator's: " + VoltDBRows.difference(admin, theirs) + "; only the " + user.name
                    + "'s: " + VoltDBRows.difference(theirs, admin));
        }
    }

    // a statement the user may run: a permission error fails the check, any other error the provider expects skips it
    private void allowed(User user, Connection connection, String sql) {
        lastQuery = sql;
        try (Statement st = connection.createStatement()) {
            st.execute(sql);
            ALLOWED.incrementAndGet();
        } catch (SQLException e) {
            if (REFUSED.matcher(String.valueOf(e.getMessage())).matches()) {
                throw new AssertionError("the " + user.name + " user was refused " + sql + ": " + e.getMessage(), e);
            }
            throw new IgnoreMeException();
        }
    }

    // a statement the user may not run: it must fail with a permission error and change no table's row count
    private void refused(User user, Connection connection, String sql) throws SQLException {
        lastQuery = sql;
        List<String> before = counts();
        String outcome = null;
        // VoltDB checks for a catalog update in progress before it checks the user's permission, so DDL can be
        // refused with "another one is in progress" first: that is retried, as the connection retries DDL
        long deadline = System.currentTimeMillis() + 60_000;
        while (true) {
            try (Statement st = connection.createStatement()) {
                st.execute(sql);
                outcome = "it ran";
            } catch (SQLException e) {
                if (VoltDBErrors.isRetryable(e) && System.currentTimeMillis() < deadline) {
                    sleep();
                    continue;
                }
                outcome = REFUSED.matcher(String.valueOf(e.getMessage())).matches() ? null
                        : "it failed without a permission error: " + e.getMessage();
            }
            break;
        }
        List<String> after = counts();
        REFUSED_CHECKS.incrementAndGet();
        if (outcome != null || !before.equals(after)) {
            throw new AssertionError("the " + user.name + " user must not run " + sql + ", but "
                    + (outcome == null ? "it" : outcome + ", and it") + " left the row counts " + after + " (before: "
                    + before + ")");
        }
    }

    // a procedure only sqlancer_read may call: the administrator creates it, the user calls it, and the administrator
    // drops it again, as a procedure that reads a table makes other oracles' DROP TABLE of it fail
    private void procedure(User user, Connection connection, VoltDBTable table) throws SQLException {
        String name = state.getDatabaseName().toUpperCase() + "_PERMS_P";
        if (!admin("CREATE PROCEDURE " + name + " ALLOW sqlancer_read AS SELECT COUNT(*) FROM " + table.getName())) {
            throw new IgnoreMeException();
        }
        lastQuery = "EXEC " + name;
        String refusal = null;
        boolean ran = false;
        try (CallableStatement call = connection.prepareCall("{call " + name + "}");
                ResultSet rs = call.executeQuery()) {
            rs.next();
            ran = true;
        } catch (SQLException e) {
            if (REFUSED.matcher(String.valueOf(e.getMessage())).matches()) {
                refusal = e.getMessage();
            }
        } finally {
            admin("DROP PROCEDURE " + name + " IF EXISTS");
        }
        if (!ran && refusal == null) {
            // the table went, or the like
            throw new IgnoreMeException();
        }
        if (user == User.READER) {
            ALLOWED.incrementAndGet();
            if (refusal != null) {
                throw new AssertionError(
                        "the reader user, whose role the procedure ALLOWs, was refused " + name + ": " + refusal);
            }
        } else {
            REFUSED_CHECKS.incrementAndGet();
            if (refusal == null) {
                throw new AssertionError("the writer user, whose role the procedure does not ALLOW, ran " + name);
            }
        }
    }

    // a statement as the administrator, retried while another catalog update is in progress; whether it ran
    private boolean admin(String sql) {
        long deadline = System.currentTimeMillis() + 60_000;
        while (true) {
            try (Statement st = state.getConnection().createStatement()) {
                st.execute(sql);
                return true;
            } catch (SQLException e) {
                if (VoltDBErrors.isRetryable(e) && System.currentTimeMillis() < deadline) {
                    sleep();
                    continue;
                }
                return false;
            }
        }
    }

    private static void sleep() {
        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IgnoreMeException();
        }
    }

    // every table's row count, as the administrator sees it
    private List<String> counts() throws SQLException {
        List<String> counts = new ArrayList<>();
        for (VoltDBTable t : state.getSchema().getDatabaseTables()) {
            try (Statement st = state.getConnection().createStatement();
                    ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM " + t.getName())) {
                rs.next();
                counts.add(t.getName() + "=" + rs.getLong(1));
            } catch (SQLException e) {
                counts.add(t.getName() + "=error: " + e.getMessage());
            }
        }
        return counts;
    }

    private interface Statements {
        Statement create() throws SQLException;
    }

    private List<String> rows(Statements statements, String sql, int columns, User user) throws SQLException {
        List<String> rows = new ArrayList<>();
        try (Statement st = statements.create(); ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                List<String> row = new ArrayList<>();
                for (int i = 1; i <= columns; i++) {
                    // the users' connections are plain JDBC, whose getObject() gives a NULL integer as its type's
                    // sentinel (ENG-4643); wasNull() is right
                    Object v = rs.getObject(i);
                    row.add(rs.wasNull() ? "null" : value(v));
                }
                rows.add(String.join(" | ", row));
            }
        } catch (SQLException e) {
            if (user != null && REFUSED.matcher(String.valueOf(e.getMessage())).matches()) {
                throw new AssertionError(
                        "the " + user.name + " user was refused a query: " + sql + ": " + e.getMessage(), e);
            }
            if (errors.errorIsExpected(e.getMessage())) {
                throw new IgnoreMeException();
            }
            throw new AssertionError(sql, e);
        }
        Collections.sort(rows);
        return rows;
    }

    private static String value(Object v) {
        if (v instanceof Double) {
            double d = (Double) v;
            return d == 0 ? "0" : String.format("%.12g", d);
        }
        if (v instanceof BigDecimal) {
            BigDecimal d = ((BigDecimal) v).stripTrailingZeros();
            return d.signum() == 0 ? "0" : d.toPlainString();
        }
        if (v instanceof byte[]) {
            StringBuilder sb = new StringBuilder("x'");
            for (byte b : (byte[]) v) {
                sb.append(String.format("%02X", b));
            }
            return sb.append('\'').toString();
        }
        return String.valueOf(v);
    }

    private static synchronized Connection connection(VoltDBGlobalState state, User user) throws SQLException {
        Connection c = CONNECTIONS[user.ordinal()];
        if (c == null || c.isClosed()) {
            String servers = state.getDbmsSpecificOptions().servers;
            String url = servers == null || servers.isBlank()
                    ? "jdbc:voltdb://" + state.getOptions().getHost() + ":" + state.getOptions().getPort()
                    : "jdbc:voltdb://" + servers.trim();
            c = DriverManager.getConnection(url, user.name, user.name);
            CONNECTIONS[user.ordinal()] = c;
        }
        return c;
    }

    @Override
    public Reproducer<VoltDBGlobalState> getLastReproducer() {
        return null;
    }

    @Override
    public String getLastQueryString() {
        return lastQuery;
    }

}
