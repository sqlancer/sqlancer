package sqlancer.voltdb.oracle;

import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.Reproducer;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.oracle.TestOracleUtils;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.schema.AbstractTables;
import sqlancer.voltdb.VoltDBConnection;
import sqlancer.voltdb.VoltDBProvider.VoltDBGlobalState;
import sqlancer.voltdb.VoltDBSchema.VoltDBColumn;
import sqlancer.voltdb.VoltDBSchema.VoltDBDataType;
import sqlancer.voltdb.VoltDBSchema.VoltDBTable;
import sqlancer.voltdb.VoltDBToStringVisitor;
import sqlancer.voltdb.ast.VoltDBExpression;
import sqlancer.voltdb.ast.VoltDBSelect;
import sqlancer.voltdb.gen.VoltDBExpressionGenerator;

/**
 * Differential query plans (DQP): the same query planned two ways must return the same rows. PLANS generates a query
 * (columns and a WHERE clause over the FROM clauses and joins the other oracles use), changes how VoltDB can plan it
 * without changing what it means, checks with {@code @Explain} that the plan did change (else the check is skipped),
 * and compares the two queries' rows as multisets. The change is one of:
 * <ul>
 * <li>{@code INDEX_OFF}: a numeric column the WHERE clause reads becomes {@code (c + 0)} there, which VoltDB can use
 * neither for an index scan nor to route the query to one partition, so an index scan becomes a sequential scan, or a
 * single-partition plan a multi-partition one;</li>
 * <li>{@code JOIN_ORDER}: the FROM list of an inner join, in reverse;</li>
 * <li>{@code INDEX_ON}: at most once per database, {@code CREATE INDEX} on a column the WHERE clause reads, dropped
 * after the check.</li>
 * </ul>
 * FLOATs are compared as DIFF compares them, and a negative zero equals zero ({@code -0.0 + 0} is {@code 0.0}). Only
 * numeric columns are wrapped: the identities of the other types run into known bugs ({@code '' || ''} is NULL,
 * ENG-30546). A failure holds both queries and both plans; it has no reproducer, as a reduction would not keep the
 * plans apart. At exit, the oracle prints how many checks compared different plans, of each kind, and how many it
 * skipped because the plan did not change; run-sqlancer.sh puts that line in its summary.
 */
public class VoltDBPlanOracle implements TestOracle<VoltDBGlobalState> {

    private enum Change {
        INDEX_OFF, JOIN_ORDER, INDEX_ON
    }

    private static final AtomicLong[] COMPARED = { new AtomicLong(), new AtomicLong(), new AtomicLong() };
    private static final AtomicLong SAME_PLAN = new AtomicLong();
    private static final Pattern WHITESPACE = Pattern.compile("\\s+");

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(
                () -> System.out.println("PLANS compared different plans: index off " + COMPARED[0] + ", join order "
                        + COMPARED[1] + ", index on " + COMPARED[2] + "; skipped as the same plan " + SAME_PLAN)));
    }

    private final VoltDBGlobalState state;
    private final ExpectedErrors errors;
    private boolean indexDone;
    private String lastQuery;

    public VoltDBPlanOracle(VoltDBGlobalState state, ExpectedErrors errors) {
        this.state = state;
        this.errors = errors;
    }

    @Override
    public void check() throws Exception {
        VoltDBExpressionGenerator gen = new VoltDBExpressionGenerator(state);
        AbstractTables<VoltDBTable, VoltDBColumn> tables = TestOracleUtils
                .getRandomTableNonEmptyTables(state.getSchema());
        gen.setTablesAndColumns(tables);
        VoltDBSelect select = gen.generateSelect();
        select.setFromList(gen.getTableRefs());
        select.setJoinClauses(gen.getRandomJoinClauses());
        List<VoltDBExpression> fetched = gen.generateFetchColumns();
        select.setFetchColumns(fetched);
        VoltDBExpression where = gen.generateBooleanExpression();
        select.setWhereClause(where);
        String query = select.asString();
        int columns = fetched.size();

        List<Change> options = new ArrayList<>(List.of(Change.INDEX_OFF));
        if (select.getJoinClauses().isEmpty() && select.getFromList().size() >= 2) {
            options.add(Change.JOIN_ORDER);
        }
        if (!indexDone) {
            options.add(Change.INDEX_ON);
        }
        Change change = Randomly.fromList(options);
        String whereSql = VoltDBToStringVisitor.asString(where);
        VoltDBColumn column = Randomly.fromList(whereColumns(tables.getColumns(), whereSql));

        String other = query;
        String planHere;
        List<String> rowsHere = null;
        String index = null;
        switch (change) {
        case INDEX_OFF:
            // ENG-30626: MOD(x, c + 0) inside LN, SQRT or EXP fails ("unsupported non-integral or non-decimal type
            // for SQL MOD function"), so a WHERE clause with MOD keeps its columns as they are
            if (state.getDbmsSpecificOptions().avoidKnownBugs && whereSql.contains("MOD(")) {
                throw new IgnoreMeException();
            }
            String wrapped = wrap(whereSql, column);
            if (wrapped.equals(whereSql) || query.indexOf(whereSql) != query.lastIndexOf(whereSql)) {
                throw new IgnoreMeException();
            }
            other = query.replace(whereSql, wrapped);
            planHere = plan(query);
            break;
        case JOIN_ORDER:
            List<VoltDBExpression> reversed = new ArrayList<>(select.getFromList());
            Collections.reverse(reversed);
            select.setFromList(reversed);
            other = select.asString();
            planHere = plan(query);
            break;
        case INDEX_ON:
            // the plan and rows without the index first
            indexDone = true;
            planHere = plan(query);
            rowsHere = rows(query, columns);
            index = state.getFreeIndexName();
            if (!ddl("CREATE INDEX " + index + " ON " + column.getTable().getName() + " (" + column.getName() + ")")) {
                throw new IgnoreMeException();
            }
            break;
        default:
            throw new AssertionError(change);
        }

        try {
            String planThere = plan(other);
            // the rows only when the plans differ: most changes leave the plan as it was
            if (shape(planHere).equals(shape(planThere))) {
                SAME_PLAN.incrementAndGet();
                throw new IgnoreMeException();
            }
            if (rowsHere == null) {
                rowsHere = rows(query, columns);
            }
            List<String> rowsThere = rows(other, columns);
            COMPARED[change.ordinal()].incrementAndGet();
            if (!rowsHere.equals(rowsThere)) {
                throw new AssertionError("different rows from different plans (" + change + ")" + System.lineSeparator()
                        + "-- query: " + query + ";" + System.lineSeparator() + "-- plan:" + System.lineSeparator()
                        + planHere + System.lineSeparator() + "-- rows: " + rowsHere.size() + System.lineSeparator()
                        + "-- other query: " + other + ";" + System.lineSeparator()
                        + (index == null ? "" : "-- (the same query, after CREATE INDEX " + index + " ON "
                                + column.getTable().getName() + " (" + column.getName() + "))" + System.lineSeparator())
                        + "-- plan:" + System.lineSeparator() + planThere + System.lineSeparator() + "-- rows: "
                        + rowsThere.size() + System.lineSeparator() + "-- only the first: "
                        + VoltDBRows.difference(rowsHere, rowsThere) + System.lineSeparator() + "-- only the other: "
                        + VoltDBRows.difference(rowsThere, rowsHere));
            }
        } finally {
            if (index != null) {
                ddl("DROP INDEX " + index + " IF EXISTS");
            }
        }
    }

    // the numeric columns of the FROM clause that the WHERE clause reads
    private static List<VoltDBColumn> whereColumns(List<VoltDBColumn> columns, String whereSql) {
        List<VoltDBColumn> numeric = columns.stream().filter(c -> isNumeric(c.getType().getType()))
                .filter(c -> reference(c).matcher(whereSql).find()).collect(Collectors.toList());
        if (numeric.isEmpty()) {
            throw new IgnoreMeException();
        }
        return numeric;
    }

    private static boolean isNumeric(VoltDBDataType type) {
        switch (type) {
        case TINYINT:
        case SMALLINT:
        case INTEGER:
        case BIGINT:
        case FLOAT:
        case DECIMAL:
            return true;
        default:
            return false;
        }
    }

    private static Pattern reference(VoltDBColumn c) {
        VoltDBTable table = c.getTable();
        return Pattern.compile("(?<![\\w.])" + Pattern.quote(table.getName() + "." + c.getName()) + "(?!\\w)");
    }

    private static String wrap(String whereSql, VoltDBColumn c) {
        return reference(c).matcher(whereSql)
                .replaceAll(Matcher.quoteReplacement("(" + c.getTable().getName() + "." + c.getName() + " + 0)"));
    }

    private boolean ddl(String sql) {
        try (Statement st = state.getConnection().createStatement()) {
            st.execute(sql);
            state.updateSchema();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    // the plan @Explain gives, its lines joined
    private String plan(String sql) throws SQLException {
        lastQuery = sql;
        VoltDBConnection connection = (VoltDBConnection) state.getConnection();
        try (CallableStatement call = connection.prepareCall("{call @Explain(?)}")) {
            call.setString(1, sql);
            try (ResultSet rs = call.executeQuery()) {
                StringBuilder sb = new StringBuilder();
                while (rs.next()) {
                    sb.append(rs.getString(1));
                }
                return sb.toString();
            }
        } catch (SQLException e) {
            if (errors.errorIsExpected(e.getMessage())) {
                throw new IgnoreMeException();
            }
            throw new AssertionError(sql, e);
        }
    }

    // a plan's nodes without their details: EXPLAIN puts each node on a line that starts with a capital letter, and
    // its predicates, ranges and parameters on lines that start with a lower-case one
    private static String shape(String plan) {
        return plan.lines().map(String::strip).filter(l -> !l.isEmpty() && Character.isUpperCase(l.charAt(0)))
                .map(l -> WHITESPACE.matcher(l).replaceAll(" ")).collect(Collectors.joining("\n"));
    }

    private List<String> rows(String sql, int columns) throws SQLException {
        lastQuery = sql;
        List<String> rows = new ArrayList<>();
        try (Statement st = state.getConnection().createStatement(); ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                List<String> row = new ArrayList<>();
                for (int i = 1; i <= columns; i++) {
                    row.add(value(rs.getObject(i)));
                }
                rows.add(String.join(" | ", row));
            }
        } catch (SQLException e) {
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

    @Override
    public Reproducer<VoltDBGlobalState> getLastReproducer() {
        return null;
    }

    @Override
    public String getLastQueryString() {
        return lastQuery;
    }

}
