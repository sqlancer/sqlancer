package sqlancer.voltdb.oracle;

import java.sql.SQLException;
import java.util.ArrayList;
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
import sqlancer.voltdb.ast.VoltDBColumnReference;
import sqlancer.voltdb.ast.VoltDBConstant;
import sqlancer.voltdb.ast.VoltDBExpression;
import sqlancer.voltdb.ast.VoltDBSelect;
import sqlancer.voltdb.gen.VoltDBExpressionGenerator;
import sqlancer.voltdb.gen.VoltDBExpressionGenerator.Kind;

/**
 * LIMIT and OFFSET. A query ordered by every one of its output columns has one order, up to rows that are the same in
 * every column. So the query with LIMIT n OFFSET m must return exactly rows m + 1 to m + n of the query without them,
 * in the same order.
 *
 * <p>
 * The query reads the same FROM clauses and joins as the other oracles, has a WHERE clause half the time and DISTINCT a
 * quarter of the time, and outputs columns or expressions of any kind but BOOLEAN, which VoltDB orders wrongly
 * (ENG-30493). ORDER BY names the outputs by position, each ascending or descending. VoltDB pushes LIMIT and OFFSET
 * down to the partitions of a multi-partition query and merges their sorted results, which is what this checks.
 */
public class VoltDBLimitOracle implements TestOracle<VoltDBGlobalState> {

    private final VoltDBGlobalState state;
    private final ExpectedErrors errors;
    private String lastQuery;
    private Reproducer<VoltDBGlobalState> reproducer;

    public VoltDBLimitOracle(VoltDBGlobalState state, ExpectedErrors errors) {
        this.state = state;
        this.errors = errors;
    }

    @Override
    public void check() throws Exception {
        reproducer = null;
        boolean distinct = Randomly.getBoolean() && Randomly.getBoolean();
        // ENG-30531: VoltDB trusts an index's order through a FULL JOIN, whose null-extended rows come last, so
        // DISTINCT
        // can return the NULL group twice and ORDER BY of partitioned tables comes out unsorted; unless
        // --avoid-known-bugs is false, there are no FULL JOINs (setNoFullJoins applies only then)
        VoltDBExpressionGenerator gen = new VoltDBExpressionGenerator(state).setNoFullJoins(true);
        gen.setTablesAndColumns(TestOracleUtils.getRandomTableNonEmptyTables(state.getSchema()));
        VoltDBSelect select = gen.generateSelect();
        select.setFromList(gen.getTableRefs());
        select.setJoinClauses(gen.getRandomJoinClauses());
        List<VoltDBExpression> outputs = new ArrayList<>();
        if (Randomly.getBoolean()) {
            // columns; with DISTINCT, ones VoltDB can group by (see generateGroupingColumns)
            List<VoltDBExpression> columns = distinct ? gen.generateGroupingColumns() : gen.generateFetchColumns();
            for (VoltDBExpression c : columns) {
                if (Kind.fromType(((VoltDBColumnReference) c).getColumn().getType().getType()) != Kind.BOOLEAN) {
                    outputs.add(c);
                }
            }
        } else {
            int n = 1 + Randomly.smallNumber();
            for (int i = 0; i < n && i < 3; i++) {
                Kind kind = Randomly.fromOptions(Kind.NUMERIC, Kind.STRING, Kind.VARBINARY, Kind.TEMPORAL);
                VoltDBExpression e = gen.generateExpressionInDomain(kind);
                // VoltDB rejects ORDER BY of a constant ("ORDER BY cannot contain constant values")
                if (!VoltDBExpressionEvaluator.isConstant(e)) {
                    outputs.add(e);
                }
            }
        }
        if (outputs.isEmpty()) {
            throw new IgnoreMeException();
        }
        // aliases also keep DISTINCT's columns of the same name apart (ENG-30529)
        select.setFetchColumns(gen.aliased(outputs));
        select.setDistinct(distinct);
        if (Randomly.getBoolean()) {
            select.setWhereClause(gen.generateBooleanExpression());
        }
        List<VoltDBExpression> orderBy = new ArrayList<>();
        for (int i = 1; i <= outputs.size(); i++) {
            orderBy.add(new VoltDBConstant(i + (Randomly.getBoolean() ? " DESC" : "")));
        }
        select.setOrderByClauses(orderBy);
        String fullQuery = select.asString();
        Randomly r = state.getRandomly();
        int limit = r.getInteger(0, 12);
        int offset = Randomly.getBoolean() ? 0 : r.getInteger(0, 12);
        select.setLimitClause(new VoltDBConstant(Integer.toString(limit)));
        if (offset > 0 || Randomly.getBoolean()) {
            select.setOffsetClause(new VoltDBConstant(Integer.toString(offset)));
        }
        LimitQueries queries = new LimitQueries(fullQuery, select.asString(), outputs.size(), limit, offset);
        lastQuery = queries.limitedQuery;

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
            throw new AssertionError("LIMIT mismatch: " + result.limited.size() + " rows, expected "
                    + result.expected.size() + " (" + result.full.size() + " without LIMIT)");
        }
    }

    // The query without and with LIMIT and OFFSET
    private final class LimitQueries {
        final String fullQuery;
        final String limitedQuery;
        final int columns;
        final int limit;
        final int offset;

        LimitQueries(String fullQuery, String limitedQuery, int columns, int limit, int offset) {
            this.fullQuery = fullQuery;
            this.limitedQuery = limitedQuery;
            this.columns = columns;
            this.limit = limit;
            this.offset = offset;
        }

        // An expected error in either query throws IgnoreMeException; an unexpected one throws AssertionError
        Result run(VoltDBGlobalState s) throws SQLException {
            List<String> full = rows(s, fullQuery);
            List<String> limited = rows(s, limitedQuery);
            int from = Math.min(offset, full.size());
            int to = Math.min(offset + limit, full.size());
            return new Result(full, new ArrayList<>(full.subList(from, to)), limited);
        }

        // each row as its values joined with " | ", a NULL as null, a FLOAT exactly, with -0.0 as 0.0: they are equal,
        // so they tie in ORDER BY and either may come first
        private List<String> rows(VoltDBGlobalState s, String sql) throws SQLException {
            if (s.getOptions().logEachSelect()) {
                s.getLogger().writeCurrent(sql);
            }
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
                        values.add(value instanceof Double ? Double.toString((Double) value + 0.0)
                                : String.valueOf(rs.getString(i)));
                    }
                    rows.add(String.join(" | ", values));
                }
            }
            return rows;
        }

        String describe() {
            return "-- full: " + fullQuery + ';' + System.lineSeparator() + "-- limited: " + limitedQuery + ';'
                    + System.lineSeparator();
        }
    }

    // The rows of the full query, the slice of them LIMIT and OFFSET select, and the limited query's rows, in order
    private static final class Result {
        final List<String> full;
        final List<String> expected;
        final List<String> limited;

        Result(List<String> full, List<String> expected, List<String> limited) {
            this.full = full;
            this.expected = expected;
            this.limited = limited;
        }

        boolean differs() {
            return !expected.equals(limited);
        }
    }

    // Re-runs the two queries and reports the bug as still present while they still differ
    private static final class MismatchReproducer implements Reproducer<VoltDBGlobalState> {
        private final LimitQueries queries;
        private Result result;

        MismatchReproducer(LimitQueries queries, Result result) {
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
            String nl = System.lineSeparator();
            return "-- On the database set up by the statements above, LIMIT " + queries.limit + " OFFSET "
                    + queries.offset + " does not select the full query's rows " + (queries.offset + 1) + " to "
                    + (queries.offset + queries.limit) + ":" + nl + queries.describe() + "-- full rows ("
                    + result.full.size() + "): " + result.full + nl + "-- expected: " + result.expected + nl
                    + "-- limited: " + result.limited + nl;
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
