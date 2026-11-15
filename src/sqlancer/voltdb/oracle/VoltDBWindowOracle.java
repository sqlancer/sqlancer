package sqlancer.voltdb.oracle;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.Reproducer;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.oracle.TestOracleUtils;
import sqlancer.common.oracle.UnexpectedErrorReproducer;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.voltdb.VoltDBProvider.VoltDBGlobalState;
import sqlancer.voltdb.VoltDBSchema.VoltDBColumn;
import sqlancer.voltdb.VoltDBSchema.VoltDBDataType;
import sqlancer.voltdb.ast.VoltDBColumnReference;
import sqlancer.voltdb.ast.VoltDBExpression;
import sqlancer.voltdb.ast.VoltDBSelect;
import sqlancer.voltdb.ast.VoltDBWindowFunction;
import sqlancer.voltdb.ast.VoltDBWindowFunction.Function;
import sqlancer.voltdb.gen.VoltDBExpressionGenerator;
import sqlancer.voltdb.gen.VoltDBExpressionGenerator.Kind;

/**
 * Checks a windowed function call against the rows of its own query. The query fetches the window's PARTITION BY and
 * ORDER BY columns and the function's argument, then the window value:
 *
 * <pre>
 * SELECT p..., o..., [arg,] f([arg]) OVER (PARTITION BY p... ORDER BY o...) FROM ... [WHERE q]
 * </pre>
 *
 * The oracle groups the rows by p, orders each group by o, and computes in Java what each row's value must be: RANK is
 * 1 plus the number of rows ordered before it, DENSE_RANK 1 plus the number of distinct o values before it, ROW_NUMBER
 * a number among those of its peers (the rows with its o values), which together are a run of consecutive numbers;
 * COUNT, SUM, MIN and MAX cover the rows up to the row and its peers, or the whole group without an ORDER BY. NULLs
 * order first, and last with DESC. The query must also return the rows of the same query without the window function.
 */
public class VoltDBWindowOracle implements TestOracle<VoltDBGlobalState> {

    private final VoltDBGlobalState state;
    private final VoltDBExpressionGenerator gen;
    private final ExpectedErrors errors;
    private String lastQuery;
    private Reproducer<VoltDBGlobalState> reproducer;

    public VoltDBWindowOracle(VoltDBGlobalState state, ExpectedErrors errors) {
        this.state = state;
        this.gen = new VoltDBExpressionGenerator(state).setSubqueries(true);
        this.errors = errors;
    }

    @Override
    public void check() throws Exception {
        reproducer = null;
        gen.setTablesAndColumns(TestOracleUtils.getRandomTableNonEmptyTables(state.getSchema()));
        List<VoltDBColumn> columns = gen.getColumns();
        // VoltDB orders a window only by integer and TIMESTAMP values ("Windowed function call expressions can have
        // only integer or TIMESTAMP value types in the ORDER BY expression of their window")
        List<VoltDBColumn> orderable = columns.stream().filter(c -> isWindowOrderable(c.getType().getType()))
                .collect(Collectors.toList());
        // VoltDB rejects GROUP BY on a BOOLEAN expression, and so, it is assumed, a window partition
        List<VoltDBColumn> partitionable = columns.stream().filter(c -> c.getType().getType() != VoltDBDataType.BOOLEAN)
                .collect(Collectors.toList());

        Function function = Randomly.fromOptions(Function.values());
        if (function.needsOrderBy() && orderable.isEmpty()) {
            throw new IgnoreMeException();
        }
        List<VoltDBExpression> partitionBy = refs(atMost(Randomly.subset(partitionable), 2));
        List<VoltDBExpression> orderBy = refs(!orderable.isEmpty() && (function.needsOrderBy() || Randomly.getBoolean())
                ? atMost(Randomly.nonEmptySubset(orderable), 3) : List.of());
        // ENG-30599: with a PARTITION BY, an equality on the partitioning column can make VoltDB take an index's
        // ascending order for the window's ORDER BY ... DESC; unless --avoid-known-bugs is false, such a window
        // orders ascending
        boolean ascendingOnly = !partitionBy.isEmpty() && state.getDbmsSpecificOptions().avoidKnownBugs;
        List<Boolean> descending = new ArrayList<>();
        orderBy.forEach(o -> descending.add(!ascendingOnly && Randomly.getBoolean()));
        VoltDBExpression argument = null;
        switch (function) {
        case COUNT:
            if (Randomly.getBoolean()) {
                argument = gen.generateWindowArgument(Randomly.fromOptions(Kind.values()));
            }
            break;
        case SUM:
            argument = gen.generateWindowArgument(Kind.NUMERIC);
            break;
        case MIN:
        case MAX:
            // VoltDB orders BOOLEAN values TRUE < FALSE where it evaluates them (ENG-30493)
            argument = gen.generateWindowArgument(
                    Randomly.fromOptions(Kind.NUMERIC, Kind.STRING, Kind.VARBINARY, Kind.TEMPORAL));
            break;
        default:
            break;
        }

        List<VoltDBExpression> fetched = new ArrayList<>(partitionBy);
        fetched.addAll(orderBy);
        if (argument != null) {
            fetched.add(argument);
        }
        VoltDBSelect select = gen.generateSelect();
        select.setFromList(gen.getTableRefs());
        select.setJoinClauses(gen.getRandomJoinClauses());
        if (Randomly.getBoolean()) {
            select.setWhereClause(gen.generateBooleanExpression());
        }
        select.setFetchColumns(fetched.isEmpty() ? refs(List.of(Randomly.fromList(columns))) : fetched);
        String plainQuery = select.asString();
        List<VoltDBExpression> withWindow = new ArrayList<>(select.getFetchColumns());
        withWindow.add(new VoltDBWindowFunction(function, argument, partitionBy, orderBy, descending));
        select.setFetchColumns(withWindow);
        String windowQuery = select.asString();
        lastQuery = windowQuery;

        Window window = new Window(function, partitionBy.size(), orderBy.size(), descending, argument != null,
                withWindow.size());
        String described = "-- with the window function: " + windowQuery + ';' + System.lineSeparator()
                + "-- without it: " + plainQuery + ';' + System.lineSeparator();
        VoltDBRecheckReproducer.Check check = s -> window.check(s, windowQuery, plainQuery);
        String problem;
        try {
            problem = check.run(state);
        } catch (AssertionError unexpectedError) {
            reproducer = new UnexpectedErrorReproducer<>(check::run,
                    TestOracleUtils.getUnexpectedErrorMessage(unexpectedError), described);
            throw unexpectedError;
        }
        if (problem != null) {
            reproducer = new VoltDBRecheckReproducer(check, described, problem);
            throw new AssertionError("window mismatch: " + problem);
        }
    }

    private static boolean isWindowOrderable(VoltDBDataType type) {
        switch (type) {
        case TINYINT:
        case SMALLINT:
        case INTEGER:
        case BIGINT:
        case TIMESTAMP:
            return true;
        default:
            return false;
        }
    }

    private static <T> List<T> atMost(List<T> list, int n) {
        return list.size() <= n ? list : list.subList(0, n);
    }

    private static List<VoltDBExpression> refs(List<VoltDBColumn> columns) {
        return columns.stream().map(VoltDBColumnReference::new).collect(Collectors.toList());
    }

    // The shape of the window query's rows: the PARTITION BY values, the ORDER BY values, the argument, the window
    // value
    private final class Window {
        final Function function;
        final int partitions;
        final int orders;
        final List<Boolean> descending;
        final boolean hasArgument;
        final int columns;

        Window(Function function, int partitions, int orders, List<Boolean> descending, boolean hasArgument,
                int columns) {
            this.function = function;
            this.partitions = partitions;
            this.orders = orders;
            this.descending = descending;
            this.hasArgument = hasArgument;
            this.columns = columns;
        }

        // null when every window value is right and the rows are those of the plain query
        String check(VoltDBGlobalState s, String windowQuery, String plainQuery) throws SQLException {
            List<List<Object>> rows = VoltDBRows.values(s, windowQuery, errors, columns);
            List<String> plain = VoltDBRows.strings(s, plainQuery, errors, columns - 1);
            List<String> withoutWindow = new ArrayList<>();
            for (List<Object> row : rows) {
                withoutWindow.add(VoltDBRows.format(row.subList(0, columns - 1)));
            }
            Collections.sort(plain);
            Collections.sort(withoutWindow);
            if (!plain.equals(withoutWindow)) {
                return "rows with the window function: " + withoutWindow.size() + ", without it: " + plain.size()
                        + System.lineSeparator() + "only with it: " + VoltDBRows.difference(withoutWindow, plain)
                        + System.lineSeparator() + "only without it: " + VoltDBRows.difference(plain, withoutWindow);
            }
            Map<List<Object>, List<List<Object>>> groups = new TreeMap<>(this::compareLists);
            for (List<Object> row : rows) {
                groups.computeIfAbsent(row.subList(0, partitions), k -> new ArrayList<>()).add(row);
            }
            for (List<List<Object>> group : groups.values()) {
                String problem = checkGroup(group);
                if (problem != null) {
                    return problem;
                }
            }
            return null;
        }

        private int compareOrder(List<Object> a, List<Object> b) {
            for (int i = 0; i < orders; i++) {
                int c = compareValues(a.get(partitions + i), b.get(partitions + i));
                if (c != 0) {
                    return descending.get(i) ? -c : c;
                }
            }
            return 0;
        }

        // One partition's rows: sorted, they form runs of peers, and each run's values follow from the rows before it
        private String checkGroup(List<List<Object>> group) {
            List<List<Object>> sorted = new ArrayList<>(group);
            sorted.sort(this::compareOrder);
            Aggregate aggregate = new Aggregate(!hasArgument);
            int distinctBefore = 0;
            for (int start = 0; start < sorted.size();) {
                int end = start + 1;
                while (end < sorted.size() && compareOrder(sorted.get(start), sorted.get(end)) == 0) {
                    end++;
                }
                if (orders == 0) {
                    end = sorted.size(); // without ORDER BY, the whole partition is one run of peers
                }
                List<List<Object>> peers = sorted.subList(start, end);
                for (List<Object> row : peers) {
                    aggregate.add(hasArgument ? row.get(partitions + orders) : null);
                }
                List<Long> rowNumbers = new ArrayList<>();
                for (List<Object> row : peers) {
                    Object actual = row.get(columns - 1);
                    String wrong;
                    switch (function) {
                    case RANK:
                        wrong = expectNumber(actual, start + 1);
                        break;
                    case DENSE_RANK:
                        wrong = expectNumber(actual, distinctBefore + 1);
                        break;
                    case ROW_NUMBER:
                        rowNumbers.add(actual == null ? null : ((Number) actual).longValue());
                        wrong = null;
                        break;
                    default:
                        wrong = aggregate.expect(function, actual);
                        break;
                    }
                    if (wrong != null) {
                        return function + " of row " + VoltDBRows.format(row) + ": " + wrong;
                    }
                }
                if (function == Function.ROW_NUMBER) {
                    List<Long> expected = new ArrayList<>();
                    for (long n = start + 1; n <= end; n++) {
                        expected.add(n);
                    }
                    if (rowNumbers.contains(null)) {
                        return "ROW_NUMBER is NULL in rows " + format(peers);
                    }
                    Collections.sort(rowNumbers);
                    if (!rowNumbers.equals(expected)) {
                        return "ROW_NUMBER of the peers " + format(peers) + ": " + rowNumbers + ", expected "
                                + expected;
                    }
                }
                distinctBefore++;
                start = end;
            }
            return null;
        }

        private String format(List<List<Object>> rows) {
            return rows.stream().map(VoltDBRows::format).collect(Collectors.toList()).toString();
        }

        private int compareLists(List<Object> a, List<Object> b) {
            for (int i = 0; i < a.size(); i++) {
                int c = compareValues(a.get(i), b.get(i));
                if (c != 0) {
                    return c;
                }
            }
            return 0;
        }
    }

    private static String expectNumber(Object actual, long expected) {
        if (actual instanceof Number && ((Number) actual).longValue() == expected) {
            return null;
        }
        return VoltDBRows.format(actual) + ", expected " + expected;
    }

    // COUNT, SUM, MIN and MAX of the argument values added so far
    private static final class Aggregate {
        // COUNT(*), which counts rows rather than values that are not NULL
        final boolean countStar;
        long rows;
        long count;
        BigDecimal exactSum;
        double floatSum;
        double floatMagnitude;
        boolean isFloat;
        Object min;
        Object max;

        Aggregate(boolean countStar) {
            this.countStar = countStar;
        }

        void add(Object value) {
            rows++;
            if (value == null) {
                return;
            }
            count++;
            if (value instanceof Number) {
                if (value instanceof Double) {
                    isFloat = true;
                    floatSum += (Double) value;
                    floatMagnitude += Math.abs((Double) value);
                } else {
                    BigDecimal v = value instanceof BigDecimal ? (BigDecimal) value
                            : BigDecimal.valueOf(((Number) value).longValue());
                    exactSum = exactSum == null ? v : exactSum.add(v);
                }
            }
            if (min == null || compareValues(value, min) < 0) {
                min = value;
            }
            if (max == null || compareValues(value, max) > 0) {
                max = value;
            }
        }

        // null when actual is the aggregate's value
        String expect(Function function, Object actual) {
            switch (function) {
            case COUNT:
                return expectNumber(actual, countStar ? rows : count);
            case SUM:
                if (count == 0) {
                    return actual == null ? null : VoltDBRows.format(actual) + ", expected NULL";
                }
                if (actual == null) {
                    return "NULL, expected " + (isFloat ? floatSum : exactSum);
                }
                if (isFloat) {
                    double a = ((Number) actual).doubleValue();
                    return Math.abs(a - floatSum) <= 1e-9 * Math.max(1, floatMagnitude) ? null
                            : a + ", expected " + floatSum;
                }
                BigDecimal a = actual instanceof BigDecimal ? (BigDecimal) actual
                        : BigDecimal.valueOf(((Number) actual).longValue());
                return a.compareTo(exactSum) == 0 ? null : a + ", expected " + exactSum;
            case MIN:
            case MAX:
                Object expected = function == Function.MIN ? min : max;
                if (expected == null ? actual == null : actual != null && compareValues(actual, expected) == 0) {
                    return null;
                }
                return VoltDBRows.format(actual) + ", expected " + VoltDBRows.format(expected);
            default:
                throw new AssertionError(function);
            }
        }
    }

    // SQL's order of two values of one type, NULL first: numbers by value (0.0 and -0.0 are equal), strings by code
    // point, as their UTF-8 bytes order, VARBINARY values by unsigned bytes, and TIMESTAMPs and DATEs in time
    static int compareValues(Object a, Object b) {
        if (a == null || b == null) {
            return a == null ? b == null ? 0 : -1 : 1;
        }
        if (a instanceof Double && b instanceof Double) {
            return Double.compare((Double) a + 0.0, (Double) b + 0.0);
        }
        if (a instanceof Number && b instanceof Number) {
            return toBigDecimal((Number) a).compareTo(toBigDecimal((Number) b));
        }
        if (a instanceof String && b instanceof String) {
            return Arrays.compare(((String) a).codePoints().toArray(), ((String) b).codePoints().toArray());
        }
        if (a instanceof byte[] && b instanceof byte[]) {
            return Arrays.compareUnsigned((byte[]) a, (byte[]) b);
        }
        if (a instanceof java.sql.Timestamp && b instanceof java.sql.Timestamp) {
            return ((java.sql.Timestamp) a).compareTo((java.sql.Timestamp) b);
        }
        if (a instanceof Date && b instanceof Date) {
            return Long.compare(((Date) a).getTime(), ((Date) b).getTime());
        }
        if (a instanceof Boolean && b instanceof Boolean) {
            return Boolean.compare((Boolean) a, (Boolean) b);
        }
        return a.toString().compareTo(b.toString());
    }

    private static BigDecimal toBigDecimal(Number n) {
        if (n instanceof BigDecimal) {
            return (BigDecimal) n;
        }
        if (n instanceof Double || n instanceof Float) {
            return new BigDecimal(n.doubleValue());
        }
        return BigDecimal.valueOf(n.longValue());
    }

    @Override
    public Reproducer<VoltDBGlobalState> getLastReproducer() {
        return reproducer;
    }

    @Override
    public String getLastQueryString() {
        return lastQuery;
    }

}
