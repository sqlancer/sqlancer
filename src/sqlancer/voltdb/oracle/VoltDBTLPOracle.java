package sqlancer.voltdb.oracle;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;

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
import sqlancer.voltdb.ast.VoltDBExpression;
import sqlancer.voltdb.ast.VoltDBSelect;
import sqlancer.voltdb.gen.VoltDBExpressionGenerator;

/**
 * Ternary Logic Partitioning. A query must return the same rows as the three queries that add a random predicate p, NOT
 * p, and p IS NULL together, since p is exactly one of TRUE, FALSE or NULL for each row or group. The {@link Mode} says
 * where p goes:
 * <ul>
 * <li>{@code WHERE}: a query without a WHERE clause, and its rows with WHERE p, WHERE NOT p and WHERE p IS NULL.</li>
 * <li>{@code DISTINCT}: a SELECT DISTINCT, or a GROUP BY of all its columns, and the same with a WHERE clause; the
 * three partitions' rows together, without duplicates, must be the original's.</li>
 * <li>{@code HAVING}: a GROUP BY query with aggregates, maybe a WHERE clause, and no HAVING clause, and its rows with
 * HAVING p, HAVING NOT p and HAVING p IS NULL, where p reads the grouping columns and aggregates.</li>
 * </ul>
 *
 * <p>
 * It differs from the shared TLP oracles in two ways that VoltDB needs or allows:
 * <ul>
 * <li>The three partitions are combined with UNION ALL only when the query reads replicated tables alone. VoltDB
 * rejects a set operation over partitioned tables ("Statements are too complex in set"), so for those the three are run
 * one after another.</li>
 * <li>Rows are compared whole, as multisets, on every fetched column rather than on the first one.</li>
 * </ul>
 *
 * <p>
 * A failure hands SQLancer a reproducer, which re-runs the same queries, so {@code --use-reducer} can reduce it.
 */
public class VoltDBTLPOracle implements TestOracle<VoltDBGlobalState> {

    public enum Mode {
        WHERE, DISTINCT, HAVING
    }

    private final Mode mode;
    private final VoltDBGlobalState state;
    private final VoltDBExpressionGenerator gen;
    private final ExpectedErrors errors;
    private String lastQuery;
    private Reproducer<VoltDBGlobalState> reproducer;

    public VoltDBTLPOracle(VoltDBGlobalState state, ExpectedErrors errors, Mode mode) {
        this.mode = mode;
        this.state = state;
        this.gen = new VoltDBExpressionGenerator(state).setNoFullJoins(mode != Mode.WHERE).setSubqueries(true);
        this.errors = errors;
    }

    @Override
    public void check() throws Exception {
        reproducer = null;
        gen.setTablesAndColumns(TestOracleUtils.getRandomTableNonEmptyTables(state.getSchema()));
        VoltDBSelect select = gen.generateSelect();
        select.setFromList(gen.getTableRefs());
        select.setJoinClauses(gen.getRandomJoinClauses());
        VoltDBExpression predicate;
        // what ORDER BY may use; for WHERE, any column, generated only when needed
        List<VoltDBExpression> orderBys = null;
        switch (mode) {
        case WHERE:
            select.setFetchColumns(gen.generateFetchColumns());
            predicate = gen.generateBooleanExpression();
            break;
        case DISTINCT:
            List<VoltDBExpression> columns = gen.generateGroupingColumns();
            select.setFetchColumns(gen.aliased(columns));
            if (Randomly.getBoolean()) {
                select.setDistinct(true);
            } else {
                select.setGroupByExpressions(columns);
            }
            predicate = gen.generateBooleanExpression();
            orderBys = Randomly.nonEmptySubset(columns);
            break;
        case HAVING:
            List<VoltDBExpression> grouping = gen.generateGroupingColumns();
            List<VoltDBExpression> fetched = new ArrayList<>(Randomly.subset(grouping));
            if (fetched.isEmpty() || Randomly.getBoolean()) {
                fetched.addAll(gen.generateAggregates());
            }
            select.setFetchColumns(gen.aliased(fetched));
            select.setGroupByExpressions(grouping);
            if (Randomly.getBoolean()) {
                select.setWhereClause(gen.generateBooleanExpression());
            }
            predicate = gen.generateHavingPredicate(grouping);
            orderBys = Randomly.nonEmptySubset(grouping);
            break;
        default:
            throw new AssertionError(mode);
        }
        String originalQuery = select.asString();

        // ORDER BY changes the plan of each partition but not its rows
        if (Randomly.getBooleanWithSmallProbability()) {
            select.setOrderByClauses(orderBys == null ? gen.generateOrderBys() : orderBys);
        }
        List<String> partitionQueries = new ArrayList<>();
        VoltDBExpression negated = mode == Mode.HAVING ? gen.negateHavingPredicate(predicate)
                : gen.negatePredicate(predicate);
        for (VoltDBExpression p : List.of(predicate, negated, gen.isNull(predicate))) {
            if (mode == Mode.HAVING) {
                select.setHavingClause(p);
            } else {
                select.setWhereClause(p);
            }
            partitionQueries.add(select.asString());
        }
        TLPQueries queries = new TLPQueries(originalQuery, partitionQueries, select.getFetchColumns().size(),
                gen.isReplicatedOnly() && select.getOrderByClauses().isEmpty());
        lastQuery = queries.combinedQuery();

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
            throw new AssertionError(
                    "TLP " + mode + " mismatch: " + result.original.size() + " rows vs " + result.combined.size());
        }
    }

    // The original query and its three partitions, and how to run them
    private final class TLPQueries {
        final String originalQuery;
        final List<String> partitionQueries;
        final int columns;
        final boolean asUnion;

        TLPQueries(String originalQuery, List<String> partitionQueries, int columns, boolean asUnion) {
            this.originalQuery = originalQuery;
            this.partitionQueries = partitionQueries;
            this.columns = columns;
            this.asUnion = asUnion;
        }

        String combinedQuery() {
            return String.join(asUnion ? mode == Mode.DISTINCT ? " UNION " : " UNION ALL " : "; ", partitionQueries);
        }

        // An expected error in any query throws IgnoreMeException; an unexpected one throws AssertionError
        Result run(VoltDBGlobalState s) throws SQLException {
            List<String> original = rows(s, originalQuery);
            List<String> combined = new ArrayList<>();
            if (asUnion) {
                combined.addAll(rows(s, combinedQuery()));
            } else {
                for (String q : partitionQueries) {
                    combined.addAll(rows(s, q));
                }
            }
            if (mode == Mode.DISTINCT) {
                // a row can be in more than one partition's DISTINCT result
                combined = new ArrayList<>(new LinkedHashSet<>(combined));
            }
            return new Result(original, combined);
        }

        // each row as its values joined with " | ", a NULL as null, and a FLOAT to 12 significant digits: a FLOAT SUM
        // in a HAVING query's output adds the same values in an order that can differ between the queries. Adding 0.0
        // makes -0.0 0.0: they are equal, so a MIN or MAX over both may return either. DISTINCT has no aggregates and
        // keeps its FLOATs exact: two values that differ only past 12 digits are two rows of the original, and rounded
        // they would be one row of the partitions once their duplicates are removed.
        private List<String> rows(VoltDBGlobalState s, String sql) throws SQLException {
            SQLQueryAdapter q = new SQLQueryAdapter(sql, errors);
            List<String> rows = new ArrayList<>();
            try (SQLancerResultSet rs = q.executeAndGet(s)) {
                if (rs == null) {
                    throw new IgnoreMeException();
                }
                while (rs.next()) {
                    List<String> values = new ArrayList<>();
                    for (int i = 1; i <= columns; i++) {
                        Object value = rs.getObject(i);
                        values.add(value instanceof Double ? formatFloat((Double) value + 0.0)
                                : String.valueOf(rs.getString(i)));
                    }
                    rows.add(String.join(" | ", values));
                }
            }
            return rows;
        }

        private String formatFloat(double value) {
            return mode == Mode.DISTINCT ? Double.toString(value) : String.format("%.12g", value);
        }

        // the queries as commented lines, each ending in a line separator
        String describe() {
            StringBuilder sb = new StringBuilder();
            sb.append("-- original: ").append(originalQuery).append(';').append(System.lineSeparator());
            if (asUnion) {
                sb.append("-- partitions: ").append(combinedQuery()).append(';').append(System.lineSeparator());
            } else {
                for (String q : partitionQueries) {
                    sb.append("-- partition: ").append(q).append(';').append(System.lineSeparator());
                }
            }
            return sb.toString();
        }
    }

    // The rows of the original query and of its partitions together, each sorted
    private static final class Result {
        final List<String> original;
        final List<String> combined;

        Result(List<String> original, List<String> combined) {
            this.original = new ArrayList<>(original);
            this.combined = new ArrayList<>(combined);
            Collections.sort(this.original);
            Collections.sort(this.combined);
        }

        boolean differs() {
            return !original.equals(combined);
        }

        // the rows of a that b lacks, counting duplicates
        static List<String> difference(List<String> a, List<String> b) {
            List<String> rest = new ArrayList<>(a);
            for (String row : b) {
                rest.remove(row);
            }
            return rest;
        }
    }

    // Re-runs the queries and reports the bug as still present while the two sides still differ. It reports the rows of
    // the last run that still differed, so that after reduction they are those of the reduced database.
    private static final class MismatchReproducer implements Reproducer<VoltDBGlobalState> {
        private final TLPQueries queries;
        private Result result;

        MismatchReproducer(TLPQueries queries, Result result) {
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
            return "-- On the database set up by the statements above, the rows of these queries mismatch:"
                    + System.lineSeparator() + queries.describe() + "-- original rows: " + result.original.size()
                    + ", partition rows: " + result.combined.size() + System.lineSeparator()
                    + "-- rows only in the original: " + Result.difference(result.original, result.combined)
                    + System.lineSeparator() + "-- rows only in the partitions: "
                    + Result.difference(result.combined, result.original) + System.lineSeparator();
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
