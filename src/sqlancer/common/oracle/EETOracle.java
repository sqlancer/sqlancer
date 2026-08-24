package sqlancer.common.oracle;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import sqlancer.ComparatorHelper;
import sqlancer.Randomly;
import sqlancer.Reproducer;
import sqlancer.SQLGlobalState;
import sqlancer.TransformationReproducer;
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

    private final class EETReproducer extends AbstractComparisonReproducer<G, List<String>>
            implements TransformationReproducer<G> {
        private final String originalQueryString;
        // Mutable: transformation reduction re-renders the transformed query with some transformation sites disabled.
        private String transformedQueryString;
        private final String initialTransformedQueryString;

        // The query parts needed to re-render the transformed query: the SELECT whose fetch columns and WHERE clause
        // are replaced, the untransformed expressions, and the records of their transformations.
        private final Z select;
        private final List<E> fetchColumns;
        private final List<EETTransformer.TransformationRecord> fetchColumnRecords;
        private final E whereClause;
        private final EETTransformer.TransformationRecord whereClauseRecord;

        EETReproducer(String originalQueryString, String transformedQueryString, Z select, List<E> fetchColumns,
                List<EETTransformer.TransformationRecord> fetchColumnRecords, E whereClause,
                EETTransformer.TransformationRecord whereClauseRecord) {
            this.originalQueryString = originalQueryString;
            this.transformedQueryString = transformedQueryString;
            this.initialTransformedQueryString = transformedQueryString;
            this.select = select;
            this.fetchColumns = fetchColumns;
            this.fetchColumnRecords = fetchColumnRecords;
            this.whereClause = whereClause;
            this.whereClauseRecord = whereClauseRecord;
        }

        @Override
        public int getTransformationSiteCount() {
            int siteCount = whereClauseRecord.getSiteCount();
            for (EETTransformer.TransformationRecord record : fetchColumnRecords) {
                siteCount += record.getSiteCount();
            }
            return siteCount;
        }

        @Override
        public Set<Integer> getDeadBranchSites() {
            // Global site indices are assigned over the fetch columns' records first (in column order), then the
            // WHERE clause's record.
            Set<Integer> deadBranchSites = new HashSet<>();
            int offset = 0;
            for (EETTransformer.TransformationRecord record : fetchColumnRecords) {
                for (int site : record.getDeadBranchSites()) {
                    deadBranchSites.add(offset + site);
                }
                offset += record.getSiteCount();
            }
            for (int site : whereClauseRecord.getDeadBranchSites()) {
                deadBranchSites.add(offset + site);
            }
            return deadBranchSites;
        }

        @Override
        public void applyTransformationSites(Set<Integer> enabledSites, Set<Integer> constantConditionSites,
                Set<Integer> copiedDeadBranchSites) {
            if (enabledSites.size() == getTransformationSiteCount() && constantConditionSites.isEmpty()
                    && copiedDeadBranchSites.isEmpty()) {
                // With every site fully enabled, the transformed query is the unreduced one; keep the exact string
                // that originally detected the bug rather than re-rendering it (rendering an AST draws random textual
                // variants, so a re-render would produce a semantically equal but untested string).
                transformedQueryString = initialTransformedQueryString;
                return;
            }
            // Pin the RNG while re-rendering so the same site configuration always yields the same query string; the
            // string tested during reduction is then exactly the string the reduced test case reports.
            transformedQueryString = Randomly.withFixedSeedRandom(() -> {
                // Global site indices are assigned over the fetch columns' records first (in column order), then the
                // WHERE clause's record.
                List<E> replayedFetchColumns = new ArrayList<>();
                int offset = 0;
                for (int i = 0; i < fetchColumns.size(); i++) {
                    replayedFetchColumns.add(transformer.replay(fetchColumns.get(i), false, fetchColumnRecords.get(i),
                            directives(enabledSites, constantConditionSites, copiedDeadBranchSites, offset)));
                    offset += fetchColumnRecords.get(i).getSiteCount();
                }
                E replayedWhereClause = transformer.replay(whereClause, true, whereClauseRecord,
                        directives(enabledSites, constantConditionSites, copiedDeadBranchSites, offset));
                select.setFetchColumns(replayedFetchColumns);
                select.setWhereClause(replayedWhereClause);
                return select.asString();
            });
        }

        // Translates the global-index site sets into a record-local directives view starting at the given offset.
        private EETTransformer.SiteDirectives directives(Set<Integer> enabledSites, Set<Integer> constantConditionSites,
                Set<Integer> copiedDeadBranchSites, int offset) {
            return new EETTransformer.SiteDirectives() {
                @Override
                public boolean isEnabled(int site) {
                    return enabledSites.contains(offset + site);
                }

                @Override
                public boolean useConstantCondition(int site) {
                    return constantConditionSites.contains(offset + site);
                }

                @Override
                public boolean useCopiedDeadBranch(int site) {
                    return copiedDeadBranchSites.contains(offset + site);
                }
            };
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
        // while the WHERE clause is evaluated in a boolean context. Each transformation's record is kept so the
        // reproducer can replay it with transformation sites disabled during reduction.
        List<E> transformedFetchColumns = new ArrayList<>();
        List<EETTransformer.TransformationRecord> fetchColumnRecords = new ArrayList<>();
        for (E fetchColumn : fetchColumns) {
            transformedFetchColumns.add(transformer.transform(fetchColumn, false));
            fetchColumnRecords.add(transformer.getLastTransformationRecord());
        }
        select.setFetchColumns(transformedFetchColumns);
        select.setWhereClause(transformer.transform(whereClause, true));
        EETTransformer.TransformationRecord whereClauseRecord = transformer.getLastTransformationRecord();

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
        reproducer = new EETReproducer(originalQueryString, transformedQueryString, select, fetchColumns,
                fetchColumnRecords, whereClause, whereClauseRecord);

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
