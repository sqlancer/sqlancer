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
 */
public class EETOracle<Z extends Select<J, E, T, C>, J extends Join<E, T, C>, E extends Expression<C>, S extends AbstractSchema<?, T>, T extends AbstractTable<C, ?, ?>, C extends AbstractTableColumn<?, ?>, G extends SQLGlobalState<?, S>>
        implements TestOracle<G> {

    private final G state;
    private EETGenerator<Z, J, E, T, C> gen;
    private final EETTransformer<E, ?> transformer;
    private final ExpectedErrors errors;

    private Reproducer<G> reproducer;
    private String generatedQueryString;

    private final class EETReproducer implements Reproducer<G> {
        private final String originalQueryString;
        // null if the original bug was a DBMS error on the original query alone
        private final String transformedQueryString;
        // null if the original bug is a result set mismatch; otherwise, the message of the
        // unexpected DBMS error that the original or transformed query triggered
        private final String expectedErrorMessage;

        EETReproducer(String originalQueryString, String transformedQueryString, String expectedErrorMessage) {
            this.originalQueryString = originalQueryString;
            this.transformedQueryString = transformedQueryString;
            this.expectedErrorMessage = expectedErrorMessage;
        }

        @Override
        public boolean bugStillTriggers(G globalState) {
            List<String> originalResultSet;
            List<String> transformedResultSet;
            try {
                // Re-execute both queries against the current (reduced) database instead of comparing
                // against a cached result set, which would be stale once statements have been removed.
                originalResultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors,
                        globalState);
                if (transformedQueryString == null) {
                    // the original bug was a DBMS error on the original query alone, which no
                    // longer occurs
                    return false;
                }
                transformedResultSet = ComparatorHelper.getResultSetFirstColumnAsString(transformedQueryString, errors,
                        globalState);
            } catch (AssertionError unexpectedError) {
                // a DBMS error reproduces the bug only if the original failure was the same error;
                // other errors are artifacts of the reduction (e.g., a removed CREATE TABLE)
                return expectedErrorMessage != null
                        && expectedErrorMessage.equals(TestOracleUtils.getUnexpectedErrorMessage(unexpectedError));
            } catch (SQLException | RuntimeException e) {
                return false;
            }
            if (expectedErrorMessage != null) {
                // the original bug was a DBMS error, which no longer occurs
                return false;
            }
            try {
                ComparatorHelper.assumeResultSetsAreEqual(originalResultSet, transformedResultSet, originalQueryString,
                        List.of(transformedQueryString), globalState);
            } catch (AssertionError resultSetMismatch) {
                return true;
            }
            return false;
        }

        @Override
        public String getBugInformation() {
            StringBuilder sb = new StringBuilder();
            if (expectedErrorMessage == null) {
                sb.append("-- On the database set up by the statements above, the result sets of the following"
                        + " queries mismatch:").append(System.lineSeparator());
            } else {
                sb.append("-- On the database set up by the statements above, the following queries trigger an"
                        + " unexpected error with message: ").append(expectedErrorMessage)
                        .append(System.lineSeparator());
            }
            sb.append("-- original: ").append(originalQueryString).append(';').append(System.lineSeparator());
            if (transformedQueryString != null) {
                sb.append("-- transformed: ").append(transformedQueryString).append(';').append(System.lineSeparator());
            }
            return sb.toString();
        }
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
            // transformedQueryString is null because no transformed query is involved
            reproducer = new EETReproducer(originalQueryString, null,
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
            reproducer = new EETReproducer(originalQueryString, transformedQueryString,
                    TestOracleUtils.getUnexpectedErrorMessage(unexpectedError));
            throw unexpectedError;
        }

        // Set the reproducer before the assertion: assumeResultSetsAreEqual throws when the bug is
        // detected, so creating the reproducer afterwards would leave it null and prevent any reduction.
        reproducer = new EETReproducer(originalQueryString, transformedQueryString, null);

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
