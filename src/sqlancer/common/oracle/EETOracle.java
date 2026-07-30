package sqlancer.common.oracle;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.ComparatorHelper;
import sqlancer.Reproducer;
import sqlancer.SQLGlobalState;
import sqlancer.common.ast.newast.Expression;
import sqlancer.common.ast.newast.Join;
import sqlancer.common.ast.newast.Select;
import sqlancer.common.gen.EETGenerator;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.schema.AbstractSchema;
import sqlancer.common.schema.AbstractTable;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.common.schema.AbstractTables;

/**
 * EET (Equivalent Expression Transformation) oracle, based on "Detecting Logic Bugs in Database Engines via Equivalent
 * Expression Transformation" (Jiang &amp; Su, OSDI'24).
 *
 * <p>
 * The oracle generates a random query and then transforms its expressions (the WHERE predicate and the fetch columns)
 * into semantically equivalent ones using {@link EETGenerator#transformExpression}. Because the transformation
 * preserves semantics, the original and the transformed query must return the same result set; any discrepancy
 * indicates a logic bug in the DBMS.
 *
 * @param <Z>
 *            the DBMS-specific SELECT statement class
 * @param <J>
 *            the DBMS-specific JOIN clause class
 * @param <E>
 *            the DBMS-specific expression class
 * @param <S>
 *            the DBMS-specific schema class
 * @param <T>
 *            the DBMS-specific table class
 * @param <C>
 *            the DBMS-specific column class
 * @param <G>
 *            the DBMS-specific global state class
 */
public class EETOracle<Z extends Select<J, E, T, C>, J extends Join<E, T, C>, E extends Expression<C>, S extends AbstractSchema<?, T>, T extends AbstractTable<C, ?, ?>, C extends AbstractTableColumn<?, ?>, G extends SQLGlobalState<?, S>>
        implements TestOracle<G> {

    private final G state;
    private EETGenerator<Z, J, E, T, C> gen;
    private final EETTransformer<E, ?> transformer;
    private final ExpectedErrors errors;

    private Reproducer<G> reproducer;
    private String generatedQueryString;

    private final class EETReproducer extends AbstractComparisonReproducer<G, List<String>> {
        private final String originalQueryString;
        private final String transformedQueryString;

        EETReproducer(String originalQueryString, String transformedQueryString) {
            this.originalQueryString = originalQueryString;
            this.transformedQueryString = transformedQueryString;
        }

        @Override
        protected List<String> evaluateOriginal(G globalState) throws SQLException {
            // Re-execute against the current (reduced) database instead of comparing against a cached result set,
            // which would be stale once statements have been removed.
            return ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, globalState);
        }

        @Override
        protected List<String> evaluateTransformed(G globalState) throws SQLException {
            return ComparatorHelper.getResultSetFirstColumnAsString(transformedQueryString, errors, globalState);
        }

        @Override
        protected boolean sidesDiffer(List<String> original, List<String> transformed, G globalState) {
            try {
                ComparatorHelper.assumeResultSetsAreEqual(original, transformed, originalQueryString,
                        List.of(transformedQueryString), globalState);
            } catch (AssertionError resultSetMismatch) {
                return true;
            }
            return false;
        }

        @Override
        protected String mismatchHeaderLine() {
            return "-- On the database set up by the statements above, the result sets of the following"
                    + " queries mismatch:";
        }

        @Override
        protected void appendQueryLines(StringBuilder sb) {
            renderQueryLines(sb, originalQueryString, transformedQueryString);
        }
    }

    // Renders the failing queries as commented lines, shared by the mismatch and the unexpected-error reproducers.
    // transformedQueryString is null when the error struck the original query before any transformation existed.
    private static void renderQueryLines(StringBuilder sb, String originalQueryString, String transformedQueryString) {
        sb.append("-- original: ").append(originalQueryString).append(';').append(System.lineSeparator());
        if (transformedQueryString != null) {
            sb.append("-- transformed: ").append(transformedQueryString).append(';').append(System.lineSeparator());
        }
    }

    // Builds the reproducer for an unexpected DBMS error, which re-runs the query (or both queries) and checks the same
    // error still fires. transformedQueryString is null when only the original query ran before the error.
    private UnexpectedErrorReproducer<G> errorReproducer(String originalQueryString, String transformedQueryString,
            String expectedErrorMessage) {
        UnexpectedErrorReproducer.Execution<G> execution = globalState -> {
            ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, globalState);
            if (transformedQueryString != null) {
                ComparatorHelper.getResultSetFirstColumnAsString(transformedQueryString, errors, globalState);
            }
        };
        StringBuilder sb = new StringBuilder();
        renderQueryLines(sb, originalQueryString, transformedQueryString);
        return new UnexpectedErrorReproducer<>(execution, expectedErrorMessage, sb.toString());
    }

    public EETOracle(G state, EETGenerator<Z, J, E, T, C> gen, ExpectedErrors expectedErrors) {
        if (state == null || gen == null || expectedErrors == null) {
            throw new IllegalArgumentException("Null variables used to initialize test oracle.");
        }
        this.state = state;
        this.gen = gen;
        this.transformer = gen.createTransformer();
        this.errors = expectedErrors;
    }

    @Override
    public void check() throws SQLException {
        reproducer = null;
        S schema = state.getSchema();
        AbstractTables<T, C> targetTables = TestOracleUtils.getRandomTableNonEmptyTables(schema);
        gen = gen.setTablesAndColumns(targetTables);

        Z select = gen.generateSelect();
        select.setJoinClauses(gen.getRandomJoinClauses());
        select.setFromList(gen.getTableRefs());
        List<E> fetchColumns = gen.generateFetchColumns(true);
        select.setFetchColumns(fetchColumns);
        E whereClause = gen.generateBooleanExpression();
        select.setWhereClause(whereClause);

        String originalQueryString = select.asString();
        generatedQueryString = originalQueryString;
        List<String> originalResultSet;
        try {
            originalResultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);
        } catch (AssertionError unexpectedError) {
            // an unexpected DBMS error on the original query alone is itself a bug worth reducing;
            // there is no transformed query yet, so only the original is replayed
            reproducer = errorReproducer(originalQueryString, null,
                    TestOracleUtils.getUnexpectedErrorMessage(unexpectedError));
            throw unexpectedError;
        }

        // Transform the query's expressions into semantically equivalent ones. Fetch columns are scalar expressions,
        // while the WHERE clause is evaluated in a boolean context.
        List<E> transformedFetchColumns = fetchColumns.stream().map(c -> transformer.transform(c, false))
                .collect(Collectors.toList());
        select.setFetchColumns(transformedFetchColumns);
        select.setWhereClause(transformer.transform(whereClause, true));

        String transformedQueryString = select.asString();
        List<String> transformedResultSet;
        try {
            transformedResultSet = ComparatorHelper.getResultSetFirstColumnAsString(transformedQueryString, errors,
                    state);
        } catch (AssertionError unexpectedError) {
            // the semantics-preserving transformation made the query trigger a DBMS error that the
            // original did not, which is a bug worth reducing
            reproducer = errorReproducer(originalQueryString, transformedQueryString,
                    TestOracleUtils.getUnexpectedErrorMessage(unexpectedError));
            throw unexpectedError;
        }

        // Set the reproducer before the assertion: assumeResultSetsAreEqual throws when the bug is
        // detected, so creating the reproducer afterwards would leave it null and prevent any reduction.
        reproducer = new EETReproducer(originalQueryString, transformedQueryString);

        ComparatorHelper.assumeResultSetsAreEqual(originalResultSet, transformedResultSet, originalQueryString,
                List.of(transformedQueryString), state);
    }

    @Override
    public Reproducer<G> getLastReproducer() {
        return reproducer;
    }

    @Override
    public String getLastQueryString() {
        return generatedQueryString;
    }
}
