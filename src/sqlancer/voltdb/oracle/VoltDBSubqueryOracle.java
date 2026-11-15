package sqlancer.voltdb.oracle;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.Reproducer;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.oracle.TestOracleUtils;
import sqlancer.common.oracle.UnexpectedErrorReproducer;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.voltdb.VoltDBProvider.VoltDBGlobalState;
import sqlancer.voltdb.ast.VoltDBBinaryOperation;
import sqlancer.voltdb.ast.VoltDBBinaryOperation.VoltDBBinaryOperator;
import sqlancer.voltdb.ast.VoltDBExpression;
import sqlancer.voltdb.ast.VoltDBSelect;
import sqlancer.voltdb.ast.VoltDBSubquery;
import sqlancer.voltdb.gen.VoltDBExpressionGenerator;

/**
 * Checks that a query whose WHERE clause holds an IN, ANY or ALL subquery returns the same rows as the query with the
 * subquery written with EXISTS (see {@link VoltDBExpressionGenerator#existsRewrite}). VoltDB plans IN subqueries by
 * turning them into EXISTS itself, and a NULL on either side, an empty subquery, or a correlated one each take a
 * different path. The subquery predicate is the WHERE clause, or ANDed with another predicate, where only the rows for
 * which it is TRUE matter.
 */
public class VoltDBSubqueryOracle implements TestOracle<VoltDBGlobalState> {

    private final VoltDBGlobalState state;
    private final VoltDBExpressionGenerator gen;
    private final ExpectedErrors errors;
    private String lastQuery;
    private Reproducer<VoltDBGlobalState> reproducer;

    public VoltDBSubqueryOracle(VoltDBGlobalState state, ExpectedErrors errors) {
        this.state = state;
        this.gen = new VoltDBExpressionGenerator(state).setSubqueries(true);
        this.errors = errors;
    }

    @Override
    public void check() throws Exception {
        reproducer = null;
        gen.setTablesAndColumns(TestOracleUtils.getRandomTableNonEmptyTables(state.getSchema()));
        VoltDBSelect select = gen.generateSelect();
        select.setFromList(gen.getTableRefs());
        select.setJoinClauses(gen.getRandomJoinClauses());
        select.setFetchColumns(gen.generateFetchColumns());
        VoltDBSubquery subquery = gen.generateSubqueryComparison();
        VoltDBExpression rewrite = gen.existsRewrite(subquery);
        VoltDBExpression other = Randomly.getBoolean() ? null : gen.generateBooleanExpression();
        boolean otherFirst = Randomly.getBoolean();
        List<String> queries = new ArrayList<>();
        for (VoltDBExpression p : List.of(subquery, rewrite)) {
            if (other != null) {
                p = otherFirst ? new VoltDBBinaryOperation(other, p, VoltDBBinaryOperator.AND)
                        : new VoltDBBinaryOperation(p, other, VoltDBBinaryOperator.AND);
            }
            select.setWhereClause(p);
            queries.add(select.asString());
        }
        String original = queries.get(0);
        String rewritten = queries.get(1);
        int columns = select.getFetchColumns().size();
        lastQuery = original;
        String described = "-- with the subquery: " + original + ';' + System.lineSeparator() + "-- with EXISTS: "
                + rewritten + ';' + System.lineSeparator();
        VoltDBRecheckReproducer.Check check = s -> compare(s, original, rewritten, columns);
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
            throw new AssertionError("subquery mismatch: " + problem);
        }
    }

    // null when the two queries return the same rows
    private String compare(VoltDBGlobalState s, String original, String rewritten, int columns) throws SQLException {
        List<String> a = VoltDBRows.strings(s, original, errors, columns);
        List<String> b = VoltDBRows.strings(s, rewritten, errors, columns);
        Collections.sort(a);
        Collections.sort(b);
        if (a.equals(b)) {
            return null;
        }
        return "rows with the subquery: " + a.size() + ", with EXISTS: " + b.size() + System.lineSeparator()
                + "only with the subquery: " + VoltDBRows.difference(a, b) + System.lineSeparator()
                + "only with EXISTS: " + VoltDBRows.difference(b, a);
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
