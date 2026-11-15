package sqlancer.voltdb.oracle;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.Reproducer;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.oracle.TestOracleUtils;
import sqlancer.common.oracle.UnexpectedErrorReproducer;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.voltdb.VoltDBProvider.VoltDBGlobalState;
import sqlancer.voltdb.VoltDBSchema.VoltDBColumn;
import sqlancer.voltdb.VoltDBSchema.VoltDBTable;
import sqlancer.voltdb.VoltDBToStringVisitor;
import sqlancer.voltdb.ast.VoltDBColumnReference;
import sqlancer.voltdb.ast.VoltDBExpression;
import sqlancer.voltdb.ast.VoltDBJoin;
import sqlancer.voltdb.ast.VoltDBSelect;
import sqlancer.voltdb.ast.VoltDBTableReference;
import sqlancer.voltdb.gen.VoltDBExpressionGenerator;
import sqlancer.voltdb.gen.VoltDBExpressionGenerator.Kind;
import sqlancer.voltdb.oracle.VoltDBExpressionEvaluator.Unpredictable;

/**
 * Pivoted Query Synthesis. It picks a pivot row, one row of each table in the FROM clause, and builds a query that must
 * return it: each random predicate, in the WHERE clause and in the ON conditions of joined replicated tables, is
 * evaluated on the pivot row by {@link VoltDBExpressionEvaluator} and rewritten to be TRUE for it (p, NOT p or p IS
 * NULL). The query fetches every column, so the pivot row can be found among its rows, and up to three random
 * expressions, whose values on the pivot row must also be the evaluator's.
 *
 * <p>
 * Partitioned tables are joined on their partitioning columns (see {@link VoltDBExpressionGenerator}), so the pivot
 * row's rows of those tables have the same partitioning value. The join types are the generator's: an INNER, LEFT or
 * FULL join returns the pivot row alike, since its ON condition is TRUE for it.
 *
 * <p>
 * Every table's rows are read and the query's rows checked in Java, which is cheap because SQLancer's tables are small.
 * An expression the evaluator cannot predict (see {@link Unpredictable}) is left out, and a predicate it cannot predict
 * skips the check. A failure hands SQLancer a reproducer for {@code --use-reducer}, which checks that the pivot row is
 * still in the tables before it re-runs the query.
 */
public class VoltDBPivotedQuerySynthesisOracle implements TestOracle<VoltDBGlobalState> {

    // the most rows the query may return, as the product of its tables' sizes
    private static final long MAX_ROWS = 50_000;
    private static final int MAX_EXPRESSIONS = 3;

    private final VoltDBGlobalState state;
    private final ExpectedErrors errors;
    private String lastQuery;
    private Reproducer<VoltDBGlobalState> reproducer;

    public VoltDBPivotedQuerySynthesisOracle(VoltDBGlobalState state, ExpectedErrors errors) {
        this.state = state;
        this.errors = errors;
    }

    @Override
    public void check() throws Exception {
        reproducer = null;
        VoltDBExpressionGenerator gen = new VoltDBExpressionGenerator(state);
        gen.setTablesAndColumns(TestOracleUtils.getRandomTableNonEmptyTables(state.getSchema()));
        List<VoltDBExpression> fromList = gen.getTableRefs();
        List<VoltDBJoin> joins = gen.getRandomJoinClauses();
        List<VoltDBTable> tables = new ArrayList<>();
        fromList.forEach(t -> tables.add(((VoltDBTableReference) t).getTable()));
        joins.forEach(j -> tables.add(j.getTable().getTable()));
        // the joins attach to the last table of the FROM list
        VoltDBTable anchor = tables.get(fromList.size() - 1);

        Map<VoltDBTable, List<Object[]>> rows = new LinkedHashMap<>();
        long product = 1;
        for (VoltDBTable t : tables) {
            List<Object[]> tableRows = readRows(state, t);
            if (tableRows.isEmpty()) {
                throw new IgnoreMeException();
            }
            rows.put(t, tableRows);
            product = Math.multiplyExact(product, tableRows.size());
            if (product > MAX_ROWS) {
                throw new IgnoreMeException();
            }
        }
        Map<VoltDBTable, Object[]> pivotRows = choosePivot(anchor, rows);
        Map<String, Object> pivot = new LinkedHashMap<>();
        pivotRows.forEach((t, values) -> {
            for (int i = 0; i < values.length; i++) {
                pivot.put(t.getColumns().get(i).getFullQualifiedName(), values[i]);
            }
        });
        VoltDBExpressionEvaluator evaluator = new VoltDBExpressionEvaluator(pivot,
                state.getDbmsSpecificOptions().avoidKnownBugs);

        for (VoltDBJoin join : joins) {
            VoltDBTable t = join.getTable().getTable();
            if (!t.isPartitioned()) {
                // a joined replicated table's ON condition is random, over it and the anchor
                List<VoltDBColumn> columns = new ArrayList<>(anchor.getColumns());
                columns.addAll(t.getColumns());
                VoltDBExpressionGenerator joinGen = new VoltDBExpressionGenerator(state).setColumns(columns);
                join.setOnClause(rectify(joinGen, joinGen.generateBooleanExpression(), evaluator));
            }
        }
        VoltDBExpression where = rectify(gen, gen.generateBooleanExpression(), evaluator);

        List<VoltDBExpression> expressions = new ArrayList<>();
        List<Object> expectedValues = new ArrayList<>();
        long n = Randomly.getNotCachedInteger(0, MAX_EXPRESSIONS + 1);
        for (int i = 0; i < n; i++) {
            Kind kind = Randomly.fromOptions(Kind.values());
            VoltDBExpression expr = gen.generateExpressionInDomain(kind);
            try {
                expectedValues.add(evaluator.evaluate(expr));
                expressions.add(expr);
            } catch (Unpredictable e) {
                // left out
            }
        }

        VoltDBSelect select = gen.generateSelect();
        List<VoltDBExpression> fetched = new ArrayList<>();
        tables.forEach(t -> t.getColumns().forEach(c -> fetched.add(new VoltDBColumnReference(c))));
        fetched.addAll(expressions);
        select.setFetchColumns(fetched);
        select.setFromList(fromList);
        select.setJoinClauses(joins);
        select.setWhereClause(where);
        if (Randomly.getBooleanWithSmallProbability()) {
            select.setOrderByClauses(gen.generateOrderBys());
        }
        List<Object> pivotValues = new ArrayList<>(pivot.values());
        PivotQuery query = new PivotQuery(select.asString(), pivotRows, pivotValues, expressions, expectedValues);
        lastQuery = query.sql;

        Result result;
        try {
            result = query.run(state);
        } catch (AssertionError unexpectedError) {
            reproducer = new UnexpectedErrorReproducer<>(query::run,
                    TestOracleUtils.getUnexpectedErrorMessage(unexpectedError), query.describe());
            throw unexpectedError;
        }
        if (result.fails()) {
            reproducer = new MismatchReproducer(query, result);
            throw new AssertionError(
                    "PQS: " + result + System.lineSeparator() + query.describe() + result.details(query));
        }
    }

    // p if it is TRUE for the pivot row, NOT p if FALSE, p IS NULL if NULL
    private static VoltDBExpression rectify(VoltDBExpressionGenerator gen, VoltDBExpression predicate,
            VoltDBExpressionEvaluator evaluator) {
        Object value;
        try {
            value = evaluator.evaluate(predicate);
        } catch (Unpredictable e) {
            throw new IgnoreMeException();
        }
        if (value == null) {
            return gen.isNull(predicate);
        }
        return (Boolean) value ? predicate : gen.negatePredicate(predicate);
    }

    // A row of each table: the anchor's has a match in every table joined to it on the partitioning columns, and
    // those tables' rows are among its matches
    private static Map<VoltDBTable, Object[]> choosePivot(VoltDBTable anchor, Map<VoltDBTable, List<Object[]>> rows) {
        List<VoltDBTable> partitionJoined = rows.keySet().stream().filter(t -> t != anchor && t.isPartitioned())
                .collect(Collectors.toList());
        List<Object[]> candidates = rows.get(anchor).stream()
                .filter(r -> partitionJoined.stream().allMatch(t -> !matches(anchor, r, t, rows.get(t)).isEmpty()))
                .collect(Collectors.toList());
        if (candidates.isEmpty()) {
            throw new IgnoreMeException();
        }
        Object[] anchorRow = Randomly.fromList(candidates);
        Map<VoltDBTable, Object[]> pivot = new LinkedHashMap<>();
        for (VoltDBTable t : rows.keySet()) {
            if (t == anchor) {
                pivot.put(t, anchorRow);
            } else if (partitionJoined.contains(t)) {
                pivot.put(t, Randomly.fromList(matches(anchor, anchorRow, t, rows.get(t))));
            } else {
                pivot.put(t, Randomly.fromList(rows.get(t)));
            }
        }
        return pivot;
    }

    // the rows of t whose partitioning value equals the anchor row's, which must not be NULL
    private static List<Object[]> matches(VoltDBTable anchor, Object[] anchorRow, VoltDBTable t, List<Object[]> rows) {
        Object key = anchorRow[partitionIndex(anchor)];
        if (key == null) {
            return List.of();
        }
        int i = partitionIndex(t);
        return rows.stream().filter(r -> VoltDBExpressionEvaluator.same(key, r[i])).collect(Collectors.toList());
    }

    private static int partitionIndex(VoltDBTable t) {
        for (int i = 0; i < t.getColumns().size(); i++) {
            if (t.getColumns().get(i).getName().equals(t.getPartitionColumn())) {
                return i;
            }
        }
        throw new AssertionError(t.getName());
    }

    // every row of the table, its columns in schema order
    private List<Object[]> readRows(VoltDBGlobalState s, VoltDBTable t) throws SQLException {
        String sql = "SELECT "
                + t.getColumns().stream().map(VoltDBColumn::getFullQualifiedName).collect(Collectors.joining(", "))
                + " FROM " + t.getName();
        return read(s, sql, t.getColumns().size());
    }

    private List<Object[]> read(VoltDBGlobalState s, String sql, int columns) throws SQLException {
        List<Object[]> rows = new ArrayList<>();
        try (SQLancerResultSet rs = new SQLQueryAdapter(sql, errors).executeAndGet(s)) {
            if (rs == null) {
                throw new IgnoreMeException();
            }
            while (rs.next()) {
                Object[] row = new Object[columns];
                for (int i = 0; i < columns; i++) {
                    row[i] = VoltDBExpressionEvaluator.fromJdbc(rs.getObject(i + 1));
                }
                rows.add(row);
            }
        }
        return rows;
    }

    // The query, the pivot row it must return, and the values its expressions must have on that row
    private final class PivotQuery {
        final String sql;
        final Map<VoltDBTable, Object[]> pivotRows;
        final List<Object> pivotValues;
        final List<VoltDBExpression> expressions;
        final List<Object> expectedValues;

        PivotQuery(String sql, Map<VoltDBTable, Object[]> pivotRows, List<Object> pivotValues,
                List<VoltDBExpression> expressions, List<Object> expectedValues) {
            this.sql = sql;
            this.pivotRows = pivotRows;
            this.pivotValues = pivotValues;
            this.expressions = expressions;
            this.expectedValues = expectedValues;
        }

        // An expected error throws IgnoreMeException; an unexpected one throws AssertionError
        Result run(VoltDBGlobalState s) throws SQLException {
            int columns = pivotValues.size();
            List<Object[]> rows = read(s, sql, columns + expressions.size());
            Object[] found = null;
            for (Object[] row : rows) {
                boolean isPivot = true;
                for (int i = 0; i < columns && isPivot; i++) {
                    isPivot = VoltDBExpressionEvaluator.same(pivotValues.get(i), row[i]);
                }
                if (!isPivot) {
                    continue;
                }
                found = row;
                if (expressionsMatch(row)) {
                    return new Result(rows.size(), row, true);
                }
            }
            return new Result(rows.size(), found, false);
        }

        private boolean expressionsMatch(Object... row) {
            for (int i = 0; i < expressions.size(); i++) {
                if (!VoltDBExpressionEvaluator.same(expectedValues.get(i), row[pivotValues.size() + i])) {
                    return false;
                }
            }
            return true;
        }

        // whether every table still holds its row of the pivot row, which reducing the statements can remove
        boolean pivotStillExists(VoltDBGlobalState s) throws SQLException {
            for (Map.Entry<VoltDBTable, Object[]> e : pivotRows.entrySet()) {
                Object[] pivotRow = e.getValue();
                boolean present = readRows(s, e.getKey()).stream().anyMatch(r -> {
                    for (int i = 0; i < r.length; i++) {
                        if (!VoltDBExpressionEvaluator.same(pivotRow[i], r[i])) {
                            return false;
                        }
                    }
                    return true;
                });
                if (!present) {
                    return false;
                }
            }
            return true;
        }

        // the query and the pivot row as commented lines, each ending in a line separator
        String describe() {
            StringBuilder sb = new StringBuilder();
            sb.append("-- query: ").append(sql).append(';').append(System.lineSeparator());
            sb.append("-- pivot row:").append(System.lineSeparator());
            pivotRows.forEach((t, values) -> {
                List<String> shown = new ArrayList<>();
                for (int i = 0; i < values.length; i++) {
                    shown.add(t.getColumns().get(i).getName() + " = " + VoltDBExpressionEvaluator.show(values[i]));
                }
                sb.append("--   ").append(t.getName()).append(": ").append(String.join(", ", shown))
                        .append(System.lineSeparator());
            });
            for (int i = 0; i < expressions.size(); i++) {
                sb.append("-- expected: ").append(VoltDBToStringVisitor.asString(expressions.get(i))).append(" = ")
                        .append(VoltDBExpressionEvaluator.show(expectedValues.get(i))).append(System.lineSeparator());
            }
            return sb.toString();
        }
    }

    // The query's row count, and its row with the pivot row's columns (null if none), whose expressions may differ
    private final class Result {
        final int rows;
        final Object[] pivotRow;
        final boolean matches;

        Result(int rows, Object[] pivotRow, boolean matches) {
            this.rows = rows;
            this.pivotRow = pivotRow == null ? null : pivotRow.clone();
            this.matches = matches;
        }

        boolean fails() {
            return !matches;
        }

        @Override
        public String toString() {
            if (pivotRow == null) {
                return "the pivot row is missing from the query's " + rows + " rows";
            }
            return "the pivot row's expressions have the wrong values";
        }

        String details(PivotQuery query) {
            StringBuilder sb = new StringBuilder("-- actual: ").append(this).append(System.lineSeparator());
            if (pivotRow != null) {
                for (int i = 0; i < query.expressions.size(); i++) {
                    sb.append("--   ").append(VoltDBToStringVisitor.asString(query.expressions.get(i))).append(" = ")
                            .append(VoltDBExpressionEvaluator.show(pivotRow[query.pivotValues.size() + i]))
                            .append(System.lineSeparator());
                }
            }
            return sb.toString();
        }
    }

    // Re-runs the query while the pivot row is still in the tables, and reports the bug as still present while the
    // query still fails the same way
    private final class MismatchReproducer implements Reproducer<VoltDBGlobalState> {
        private final PivotQuery query;
        private Result result;

        MismatchReproducer(PivotQuery query, Result result) {
            this.query = query;
            this.result = result;
        }

        @Override
        public boolean bugStillTriggers(VoltDBGlobalState globalState) {
            try {
                if (!query.pivotStillExists(globalState)) {
                    return false;
                }
                Result rerun = query.run(globalState);
                if (rerun.fails() && (rerun.pivotRow == null) == (result.pivotRow == null)) {
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
            return "-- On the database set up by the statements above, this query does not return the pivot row as"
                    + " expected:" + System.lineSeparator() + query.describe() + result.details(query);
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
