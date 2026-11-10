package sqlancer.voltdb.oracle;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import sqlancer.IgnoreMeException;
import sqlancer.Reproducer;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.oracle.TestOracleUtils;
import sqlancer.common.oracle.UnexpectedErrorReproducer;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.voltdb.VoltDBProvider.VoltDBGlobalState;
import sqlancer.voltdb.ast.VoltDBAggregate;
import sqlancer.voltdb.ast.VoltDBAggregate.VoltDBAggregateFunction;
import sqlancer.voltdb.ast.VoltDBExpression;
import sqlancer.voltdb.ast.VoltDBSelect;
import sqlancer.voltdb.gen.VoltDBExpressionGenerator;

/**
 * Ternary Logic Partitioning of aggregates. An aggregate over all rows can be computed from the same aggregate over the
 * three partitions WHERE p, WHERE NOT p and WHERE p IS NULL: MIN of the MINs, MAX of the MAXes, the sum of the SUMs and
 * of the COUNTs. A query of 1 to 3 aggregates without a WHERE clause is compared with the three partition queries.
 *
 * <p>
 * The shared oracles combine the partitions in SQL, as an outer aggregate over a UNION ALL. VoltDB rejects that over
 * partitioned tables ("Statements are too complex in set"), so this oracle combines them in Java, for every query. AVG
 * is left out, as it cannot be combined from the partitions' AVGs.
 */
public class VoltDBTLPAggregateOracle implements TestOracle<VoltDBGlobalState> {

    // the difference two FLOAT SUMs may have, as they add the same values in a different order, relative to the
    // largest magnitude among them and the partitions' SUMs: when large terms cancel, the rounding error is relative
    // to the terms, not to the small result (lab build #25: partitions of +-1.49e9 summing to 2.53, off by 1e-6)
    private static final double FLOAT_TOLERANCE = 1e-9;

    private final VoltDBGlobalState state;
    private final VoltDBExpressionGenerator gen;
    private final ExpectedErrors errors;
    private String lastQuery;
    private Reproducer<VoltDBGlobalState> reproducer;

    public VoltDBTLPAggregateOracle(VoltDBGlobalState state, ExpectedErrors errors) {
        this.state = state;
        this.gen = new VoltDBExpressionGenerator(state).setUngroupedAggregates(true).setSubqueries(true);
        this.errors = errors;
    }

    @Override
    public void check() throws Exception {
        reproducer = null;
        gen.setTablesAndColumns(TestOracleUtils.getRandomTableNonEmptyTables(state.getSchema()));
        VoltDBSelect select = gen.generateSelect();
        List<VoltDBExpression> aggregates = gen.generateAggregates();
        select.setFetchColumns(aggregates);
        select.setFromList(gen.getTableRefs());
        select.setJoinClauses(gen.getRandomJoinClauses());
        String originalQuery = select.asString();

        VoltDBExpression predicate = gen.generateBooleanExpression();
        List<String> partitionQueries = new ArrayList<>();
        for (VoltDBExpression where : List.of(predicate, gen.negatePredicate(predicate), gen.isNull(predicate))) {
            select.setWhereClause(where);
            partitionQueries.add(select.asString());
        }
        List<VoltDBAggregateFunction> functions = new ArrayList<>();
        for (VoltDBExpression a : aggregates) {
            functions.add(((VoltDBAggregate) a).getFunc());
        }
        AggregateQueries queries = new AggregateQueries(originalQuery, partitionQueries, functions);
        lastQuery = originalQuery;

        Result result;
        try {
            result = queries.run(state);
        } catch (AssertionError unexpectedError) {
            reproducer = new UnexpectedErrorReproducer<>(queries::run,
                    TestOracleUtils.getUnexpectedErrorMessage(unexpectedError), queries.describe());
            throw unexpectedError;
        }
        if (result.differs()) {
            reproducer = new MismatchReproducer(queries, result);
            throw new AssertionError("TLP aggregate mismatch: " + result);
        }
    }

    // The original query and its three partitions, and how to run them
    private final class AggregateQueries {
        final String originalQuery;
        final List<String> partitionQueries;
        final List<VoltDBAggregateFunction> functions;

        AggregateQueries(String originalQuery, List<String> partitionQueries, List<VoltDBAggregateFunction> functions) {
            this.originalQuery = originalQuery;
            this.partitionQueries = partitionQueries;
            this.functions = functions;
        }

        // An expected error in any query throws IgnoreMeException; an unexpected one throws AssertionError
        Result run(VoltDBGlobalState s) throws SQLException {
            List<Object> original = row(s, originalQuery);
            List<List<Object>> partitions = new ArrayList<>();
            for (String q : partitionQueries) {
                partitions.add(row(s, q));
            }
            List<Object> combined = new ArrayList<>();
            for (int i = 0; i < functions.size(); i++) {
                List<Object> values = new ArrayList<>();
                for (List<Object> p : partitions) {
                    values.add(p.get(i));
                }
                combined.add(combine(functions.get(i), values));
            }
            return new Result(functions, original, combined, partitions);
        }

        // the query's only row
        private List<Object> row(VoltDBGlobalState s, String sql) throws SQLException {
            SQLQueryAdapter q = new SQLQueryAdapter(sql, errors);
            List<Object> values = new ArrayList<>();
            try (SQLancerResultSet rs = q.executeAndGet(s)) {
                if (rs == null) {
                    throw new IgnoreMeException();
                }
                if (rs.next()) {
                    for (int i = 1; i <= functions.size(); i++) {
                        values.add(rs.getObject(i));
                    }
                } else if (sql.equals(originalQuery)) {
                    throw new AssertionError("an aggregate query without GROUP BY returned no row: " + sql);
                } else {
                    // ENG-19957 (closed, won't fix): when a WHERE clause folds to FALSE, the planner adds LIMIT 0 and
                    // an aggregate query returns no row instead of one with COUNT 0 and NULL for the others
                    for (VoltDBAggregateFunction f : functions) {
                        values.add(f == VoltDBAggregateFunction.COUNT ? 0L : null);
                    }
                }
            }
            return values;
        }

        // the queries as commented lines, each ending in a line separator
        String describe() {
            StringBuilder sb = new StringBuilder();
            sb.append("-- original: ").append(originalQuery).append(';').append(System.lineSeparator());
            for (String q : partitionQueries) {
                sb.append("-- partition: ").append(q).append(';').append(System.lineSeparator());
            }
            return sb.toString();
        }
    }

    // The aggregate over all rows from its values over the partitions, which are NULL for MIN, MAX and SUM of no rows
    static Object combine(VoltDBAggregateFunction function, List<Object> values) {
        Object result = null;
        for (Object v : values) {
            if (v == null) {
                continue;
            }
            if (result == null) {
                result = v;
                continue;
            }
            switch (function) {
            case COUNT:
            case SUM:
                if (result instanceof Double || v instanceof Double) {
                    result = ((Number) result).doubleValue() + ((Number) v).doubleValue();
                } else {
                    // exact, as VoltDB's BIGINT and DECIMAL SUMs are; one that overflows fails in VoltDB too
                    result = decimal(result).add(decimal(v));
                }
                break;
            case MIN:
                result = compare(v, result) < 0 ? v : result;
                break;
            case MAX:
                result = compare(v, result) > 0 ? v : result;
                break;
            default:
                throw new AssertionError(function);
            }
        }
        if (result == null && function == VoltDBAggregateFunction.COUNT) {
            return 0L;
        }
        return result;
    }

    private static BigDecimal decimal(Object number) {
        return number instanceof BigDecimal ? (BigDecimal) number : new BigDecimal(number.toString());
    }

    // VoltDB orders numbers by value, VARCHAR values by their UTF-8 bytes and VARBINARY values by their bytes, unsigned
    static int compare(Object a, Object b) {
        if (a instanceof Double || b instanceof Double) {
            return Double.compare(((Number) a).doubleValue(), ((Number) b).doubleValue());
        }
        if (a instanceof Number) {
            return decimal(a).compareTo(decimal(b));
        }
        if (a instanceof String) {
            return Arrays.compareUnsigned(((String) a).getBytes(StandardCharsets.UTF_8),
                    ((String) b).getBytes(StandardCharsets.UTF_8));
        }
        if (a instanceof byte[]) {
            return Arrays.compareUnsigned((byte[]) a, (byte[]) b);
        }
        @SuppressWarnings("unchecked")
        Comparable<Object> c = (Comparable<Object>) a;
        return c.compareTo(b);
    }

    static boolean same(Object a, Object b) {
        return same(a, b, 0);
    }

    // magnitude: the largest absolute value of the FLOAT values combined into a or b, which bounds their rounding
    static boolean same(Object a, Object b, double magnitude) {
        if (a == null || b == null) {
            return a == b;
        }
        if (a instanceof Double || b instanceof Double) {
            double x = ((Number) a).doubleValue();
            double y = ((Number) b).doubleValue();
            double scale = Math.max(magnitude, Math.max(Math.abs(x), Math.abs(y)));
            return x == y || Math.abs(x - y) <= FLOAT_TOLERANCE * scale;
        }
        return compare(a, b) == 0;
    }

    private static String show(Object v) {
        if (v instanceof byte[]) {
            StringBuilder sb = new StringBuilder("x'");
            for (byte b : (byte[]) v) {
                sb.append(String.format("%02X", b));
            }
            return sb.append('\'').toString();
        }
        return String.valueOf(v);
    }

    private static String show(List<Object> values) {
        List<String> shown = new ArrayList<>();
        for (Object v : values) {
            shown.add(show(v));
        }
        return String.join(" | ", shown);
    }

    // The original query's aggregates, the ones combined from the partitions, and the partitions' own
    private static final class Result {
        final List<VoltDBAggregateFunction> functions;
        final List<Object> original;
        final List<Object> combined;
        final List<List<Object>> partitions;

        Result(List<VoltDBAggregateFunction> functions, List<Object> original, List<Object> combined,
                List<List<Object>> partitions) {
            this.functions = functions;
            this.original = original;
            this.combined = combined;
            this.partitions = partitions;
        }

        boolean differs() {
            for (int i = 0; i < original.size(); i++) {
                double magnitude = 0;
                for (List<Object> p : partitions) {
                    if (p.get(i) instanceof Double) {
                        magnitude = Math.max(magnitude, Math.abs((Double) p.get(i)));
                    }
                }
                if (!same(original.get(i), combined.get(i), magnitude)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public String toString() {
            return show(original) + " vs " + show(combined);
        }

        String details() {
            StringBuilder sb = new StringBuilder();
            sb.append("-- aggregates: ").append(functions).append(System.lineSeparator());
            sb.append("-- original: ").append(show(original)).append(System.lineSeparator());
            sb.append("-- combined: ").append(show(combined)).append(System.lineSeparator());
            for (List<Object> p : partitions) {
                sb.append("-- partition: ").append(show(p)).append(System.lineSeparator());
            }
            return sb.toString();
        }
    }

    // Re-runs the queries and reports the bug as still present while the aggregates still differ, reporting the values
    // of the last run that still differed
    private static final class MismatchReproducer implements Reproducer<VoltDBGlobalState> {
        private final AggregateQueries queries;
        private Result result;

        MismatchReproducer(AggregateQueries queries, Result result) {
            this.queries = queries;
            this.result = result;
        }

        @Override
        public boolean bugStillTriggers(VoltDBGlobalState globalState) {
            try {
                Result rerun = queries.run(globalState);
                if (rerun.differs()) {
                    result = rerun;
                    return true;
                }
                return false;
            } catch (AssertionError | SQLException | RuntimeException e) {
                // an error, expected or not, is an artifact of the reduction, such as a removed table
                return false;
            }
        }

        @Override
        public String getBugInformation() {
            return "-- On the database set up by the statements above, the aggregates of these queries mismatch:"
                    + System.lineSeparator() + queries.describe() + result.details();
        }
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
