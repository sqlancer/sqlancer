package sqlancer.common.oracle;

import java.sql.SQLException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.Reproducer;
import sqlancer.SQLGlobalState;
import sqlancer.TransformationReproducer;
import sqlancer.common.ast.newast.Expression;
import sqlancer.common.gen.EETDMLGenerator;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.common.schema.AbstractSchema;
import sqlancer.common.schema.AbstractTable;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.common.schema.AbstractTables;

/**
 * EET (Equivalent Expression Transformation) oracle for DML statements, based on "Detecting Logic Bugs in Database
 * Engines via Equivalent Expression Transformation" (Jiang &amp; Su, OSDI'24).
 *
 * <p>
 * Whereas {@link EETOracle} transforms a SELECT and compares the two result sets, this oracle transforms a DML
 * statement and compares the two database states produced.
 *
 * <p>
 * Adapted from the DQE oracle, state is observed with an auxiliary column ({@link EETDMLGenerator#ROW_ID_COLUMN}) which
 * uniquely identifies each row, and each statement is executed inside a transaction that is rolled back, so the two
 * statements can be compared against the same starting state without permanently modifying the database. The state is
 * captured as a full post-image: each surviving row's identifier together with its content column values, ordered by
 * the identifier. This single value-level surface covers every DML statement — a DELETE removes rows from it, an UPDATE
 * changes values in it, an INSERT adds rows to it (row identity alone would suffice for DELETE, but not for UPDATE,
 * which also transforms the written values). Because rolling back a statement requires a transactional storage engine,
 * the DBMS-specific setup must ensure only such engines are used while this oracle is active.
 *
 * <p>
 * DELETE, UPDATE and INSERT are currently supported (one is chosen at random per check). INSERT uses the
 * {@code INSERT ... SELECT} form so its transformed value expressions may reference columns; each inserted row is given
 * a deterministic identifier derived from its source row so the two runs' post-images align. To support reduction, a
 * {@link Reproducer} replays the whole comparison (adding and stamping the row-identifier column, running both
 * statements in rolled-back transactions and comparing the post-images) against the reduced database.
 *
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
public class EETDMLOracle<E extends Expression<C>, S extends AbstractSchema<?, T>, T extends AbstractTable<C, ?, ?>, C extends AbstractTableColumn<?, ?>, G extends SQLGlobalState<?, S>>
        implements TestOracle<G> {

    private final G state;
    private EETDMLGenerator<E, T, C> gen;
    private final EETTransformer<E, ?> transformer;
    private final ExpectedErrors errors;

    private static final int MAX_DIFF_ROWS_REPORTED = 10; // max differing post-image rows displayed in report log
    private String generatedQueryString;
    private Reproducer<G> reproducer;

    // The SQL and metadata to run and observe one DML comparison, captured as strings so a reproducer can replay it
    // against a reduced database without the generator or live schema objects.
    private static final class ComparisonQueries {
        private final String originalStatement;
        private final String transformedStatement;
        private final String addRowIdColumn;
        private final String stampRowIds;
        private final String beginTransaction;
        private final String rollback;
        private final String dropRowIdColumn;
        private final String selectPostImage;
        // The post-image's columns, in the order selectPostImage returns them: their count is how many columns each
        // row is read back with, and their names head the differing rows a mismatch is reported with.
        private final List<String> postImageColumns;

        ComparisonQueries(String originalStatement, String transformedStatement, String addRowIdColumn,
                String stampRowIds, String beginTransaction, String rollback, String dropRowIdColumn,
                String selectPostImage, List<String> postImageColumns) {
            this.originalStatement = originalStatement;
            this.transformedStatement = transformedStatement;
            this.addRowIdColumn = addRowIdColumn;
            this.stampRowIds = stampRowIds;
            this.beginTransaction = beginTransaction;
            this.rollback = rollback;
            this.dropRowIdColumn = dropRowIdColumn;
            this.selectPostImage = selectPostImage;
            this.postImageColumns = postImageColumns;
        }

        // A copy differing only in the transformed statement, used when transformation reduction re-renders it.
        ComparisonQueries withTransformedStatement(String newTransformedStatement) {
            return new ComparisonQueries(originalStatement, newTransformedStatement, addRowIdColumn, stampRowIds,
                    beginTransaction, rollback, dropRowIdColumn, selectPostImage, postImageColumns);
        }
    }

    // Records the transformations applied to a DML statement's expressions so the transformed statement can be
    // re-rendered with any subset of the transformation sites disabled or simplified (for transformation reduction).
    // The transformable expressions are held in a fixed order, each assigned a contiguous block of global site indices;
    // reassemble rebuilds the DML statement string from the (replayed) expressions in that same order. Each statement
    // kind's generator method documents the order it puts its expressions in.
    private static final class DMLTransformation<E extends Expression<?>> {
        private final EETTransformer<E, ?> transformer;
        private final List<E> originalExpressions;
        private final List<Boolean> booleanContexts;
        private final List<EETTransformer.TransformationRecord> records;
        private final Function<List<E>, String> reassemble;

        DMLTransformation(EETTransformer<E, ?> transformer, List<E> originalExpressions, List<Boolean> booleanContexts,
                List<EETTransformer.TransformationRecord> records, Function<List<E>, String> reassemble) {
            this.transformer = transformer;
            this.originalExpressions = originalExpressions;
            this.booleanContexts = booleanContexts;
            this.records = records;
            this.reassemble = reassemble;
        }

        int getSiteCount() {
            int siteCount = 0;
            for (EETTransformer.TransformationRecord record : records) {
                siteCount += record.getSiteCount();
            }
            return siteCount;
        }

        Set<Integer> getDeadBranchSites() {
            Set<Integer> deadBranchSites = new HashSet<>();
            int offset = 0;
            for (EETTransformer.TransformationRecord record : records) {
                for (int site : record.getDeadBranchSites()) {
                    deadBranchSites.add(offset + site);
                }
                offset += record.getSiteCount();
            }
            return deadBranchSites;
        }

        // Re-renders the transformed statement with the given per-site configuration; each expression is replayed with
        // its record and the block of global site indices starting at its running offset.
        String render(Set<Integer> enabledSites, Set<Integer> constantConditionSites,
                Set<Integer> copiedDeadBranchSites) {
            List<E> replayed = new ArrayList<>();
            int offset = 0;
            for (int i = 0; i < originalExpressions.size(); i++) {
                replayed.add(transformer.replay(originalExpressions.get(i), booleanContexts.get(i), records.get(i),
                        EETTransformer.SiteDirectives.forSites(enabledSites, constantConditionSites,
                                copiedDeadBranchSites, offset)));
                offset += records.get(i).getSiteCount();
            }
            return reassemble.apply(replayed);
        }
    }

    // The post-images the original and transformed statements produced, compared for equality to detect the bug.
    private static final class PostImages {
        private final List<List<String>> original;
        private final List<List<String>> transformed;

        PostImages(List<List<String>> original, List<List<String>> transformed) {
            this.original = original;
            this.transformed = transformed;
        }
    }

    // Reproduces a post-image mismatch against the reduced database. Unlike EETOracle's comparison reproducer this does
    // not extend AbstractComparisonReproducer: the two sides are not independent, because the row-id stamping (UUID())
    // must run once so both observe the same rows, so both post-images are computed together. Implements
    // TransformationReproducer so the transformed statement can be reduced by disabling and simplifying its
    // transformation sites, mirroring EETOracle.
    private final class EETDMLReproducer implements TransformationReproducer<G> {
        private final ComparisonQueries baseQueries;
        private final DMLTransformation<E> transformation;
        private final String initialTransformedStatement;
        // Mutable: transformation reduction re-renders the transformed statement with some sites disabled/simplified.
        private String transformedStatement;
        // Mutable: the post-images of the latest run that still showed the mismatch, initially the ones the oracle
        // itself observed. The reducers accept a candidate exactly when bugStillTriggers reports the mismatch, and
        // leave the reduced test case at the last accepted candidate, so these are the images of the comparison the
        // bug information ends up describing.
        private PostImages mismatchImages;

        EETDMLReproducer(ComparisonQueries queries, DMLTransformation<E> transformation, PostImages mismatchImages) {
            this.baseQueries = queries;
            this.transformation = transformation;
            this.initialTransformedStatement = queries.transformedStatement;
            this.transformedStatement = queries.transformedStatement;
            this.mismatchImages = mismatchImages;
        }

        private ComparisonQueries currentQueries() {
            return baseQueries.withTransformedStatement(transformedStatement);
        }

        @Override
        public boolean bugStillTriggers(G globalState) {
            PostImages images;
            try {
                images = computePostImages(globalState, currentQueries());
            } catch (AssertionError | SQLException | RuntimeException e) {
                // any failure re-running the comparison means this reduced database no longer shows the mismatch
                return false;
            }
            if (images.original.equals(images.transformed)) {
                return false;
            }
            mismatchImages = images;
            return true;
        }

        @Override
        public int getTransformationSiteCount() {
            return transformation.getSiteCount();
        }

        @Override
        public Set<Integer> getDeadBranchSites() {
            return transformation.getDeadBranchSites();
        }

        @Override
        public void applyTransformationSites(Set<Integer> enabledSites, Set<Integer> constantConditionSites,
                Set<Integer> copiedDeadBranchSites) {
            if (enabledSites.size() == getTransformationSiteCount() && constantConditionSites.isEmpty()
                    && copiedDeadBranchSites.isEmpty()) {
                // With every site fully enabled, keep the exact string that originally detected the bug rather than
                // re-rendering it (rendering an AST draws random textual variants, so a re-render would produce a
                // semantically equal but untested string).
                transformedStatement = initialTransformedStatement;
                return;
            }
            // Pin the RNG while re-rendering so the same site configuration always yields the same statement string;
            // the string tested during reduction is then exactly the string the reduced test case reports.
            transformedStatement = Randomly.withFixedSeedRandom(
                    () -> transformation.render(enabledSites, constantConditionSites, copiedDeadBranchSites));
        }

        @Override
        public String getBugInformation() {
            ComparisonQueries queries = currentQueries();
            StringBuilder sb = new StringBuilder();
            sb.append("-- On the database set up by the statements above, the original and transformed statements below"
                    + " leave the database in different states.").append(System.lineSeparator());
            renderStatementLines(sb, queries);
            appendDiffRows(sb, queries, mismatchImages);
            return sb.toString();
        }
    }

    // Builds the reproducer for an unexpected DBMS error, which replays the whole comparison and checks the same error
    // still fires.
    private UnexpectedErrorReproducer<G> errorReproducer(ComparisonQueries queries, String expectedErrorMessage) {
        UnexpectedErrorReproducer.Execution<G> execution = globalState -> computePostImages(globalState, queries);
        StringBuilder sb = new StringBuilder();
        renderStatementLines(sb, queries);
        return new UnexpectedErrorReproducer<>(execution, expectedErrorMessage, sb.toString());
    }

    /**
     * Renders the whole comparison, shared by the mismatch message and both reproducers. Every statement the oracle ran
     * is listed, in the order it ran, as runnable SQL: only the explanatory lines around them are commented out, so the
     * block can be selected and run as-is to reproduce the comparison by hand. The two post-image SELECTs it contains
     * return the states being compared.
     *
     * <p>
     * The two DML statements alone would not be runnable. They reference the auxiliary {@code rowid} column (in their
     * ORDER BY tiebreaker, and, for INSERT, in their column list), which the oracle adds and drops around the
     * comparison rather than leaving in the schema, so it appears nowhere in the setup statements a test case reports.
     *
     * @param sb
     *            the builder to append to
     * @param queries
     *            the statements and auxiliary SQL the comparison ran
     */
    private static void renderStatementLines(StringBuilder sb, ComparisonQueries queries) {
        sb.append("-- The statements below reproduce the comparison. They add the"
                + " auxiliary row-identifier column the two statements reference (which is not part of the schema"
                + " above) and drop it again, so run them as a whole. The two post-image SELECTs return the states"
                + " being compared:").append(System.lineSeparator());
        renderStatement(sb, queries.addRowIdColumn);
        renderStatement(sb, queries.stampRowIds);
        renderSide(sb, "original", queries.originalStatement, queries);
        renderSide(sb, "transformed", queries.transformedStatement, queries);
        renderStatement(sb, queries.dropRowIdColumn);
    }

    // Renders one side of the comparison: its DML statement run inside a rolled-back transaction, with the post-image
    // read back before the rollback undoes it.
    private static void renderSide(StringBuilder sb, String label, String statement, ComparisonQueries queries) {
        sb.append("-- ").append(label).append(':').append(System.lineSeparator());
        renderStatement(sb, queries.beginTransaction);
        renderStatement(sb, statement);
        renderStatement(sb, queries.selectPostImage);
        renderStatement(sb, queries.rollback);
    }

    private static void renderStatement(StringBuilder sb, String statement) {
        sb.append(statement).append(';').append(System.lineSeparator());
    }

    public EETDMLOracle(G state, EETDMLGenerator<E, T, C> gen, ExpectedErrors expectedErrors) {
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
        List<T> tables = state.getSchema().getDatabaseTables();
        if (tables.isEmpty()) {
            throw new IgnoreMeException();
        }
        // A DML statement targets a single table, so operate on exactly one; confining the generator to it keeps the
        // predicate and value expressions from referencing another table's columns (which would render invalid
        // single-table DML).
        T table = Randomly.fromList(tables);
        gen = gen.setTablesAndColumns(new AbstractTables<>(List.of(table)));

        E predicate = gen.generateBooleanExpression();
        // The WHERE predicate is evaluated in a boolean context.
        E transformedPredicate = transformer.transform(predicate, true);
        EETTransformer.TransformationRecord predicateRecord = transformer.getLastTransformationRecord();

        // Optionally cap the statement with a LIMIT. The limit and its ordering (a random column subset, made a total
        // order by the row-id tiebreaker) are decided once and applied identically to both statements, so the capped
        // row set is deterministic and equal across the runs while still exercising varied orderings.
        Integer limit = null;
        List<C> orderByColumns = List.of();
        if (Randomly.getBoolean()) {
            limit = (int) Randomly.getNotCachedInteger(0, 10);
            orderByColumns = Randomly.subset(table.getColumns());
        }
        // Generators for the different kinds of statement this oracle supports. One is chosen at random per check
        List<DMLStatementGenerator<E, T, C>> statementGenerators = List.of(this::generateDeleteStatements,
                this::generateUpdateStatements, this::generateInsertStatements);
        StatementPair<E> statements = Randomly.fromList(statementGenerators).generate(table, predicate,
                transformedPredicate, predicateRecord, orderByColumns, limit);
        String originalStatement = statements.original;
        String transformedStatement = statements.transformed;
        generatedQueryString = originalStatement;

        // Capture, as strings, everything needed to run and observe this comparison: the two statements plus the
        // auxiliary-column setup, per-run snapshot and teardown. A reproducer replays these against a reduced database,
        // where the live generator and schema objects no longer apply.
        ComparisonQueries queries = new ComparisonQueries(originalStatement, transformedStatement,
                gen.addRowIdColumnStatement(table), gen.stampRowIdsStatement(table), gen.beginTransactionStatement(),
                gen.rollbackTransactionStatement(), gen.dropRowIdColumnStatement(table),
                gen.selectPostImageStatement(table), gen.postImageColumns(table));

        PostImages images;
        try {
            images = computePostImages(state, queries);
        } catch (AssertionError unexpectedError) {
            reproducer = errorReproducer(queries, TestOracleUtils.getUnexpectedErrorMessage(unexpectedError));
            throw unexpectedError;
        }

        reproducer = new EETDMLReproducer(queries, statements.transformation, images);
        if (!images.original.equals(images.transformed)) {
            throw new AssertionError(mismatchMessage(queries, images));
        }
    }

    /**
     * Runs the whole comparison against {@code globalState}: adds and stamps the row-identifier column once (so both
     * runs observe the same rows), snapshots the post-image each statement produces (each inside a rolled-back
     * transaction), and drops the column. Both {@link #check()} and the reproducers call this, the former against the
     * live database and the latter against a reduced one. A DBMS error the oracle tolerates aborts with
     * {@link IgnoreMeException}; an oracle logic bug or unexpected error surfaces as {@link AssertionError}.
     *
     * @param globalState
     *            the state whose connection the comparison runs against
     * @param queries
     *            the statements and auxiliary SQL to run
     *
     * @return the post-images the original and transformed statements produced
     *
     * @throws SQLException
     *             if a DBMS interaction fails
     */
    private PostImages computePostImages(G globalState, ComparisonQueries queries) throws SQLException {
        // Add the auxiliary column outside the try, then guard everything after it with the finally that drops it: the
        // ALTER auto-commits (it is not undone by ROLLBACK), so a failure between adding and dropping would leak the
        // column and cause cascading duplicate-column failures
        if (!new SQLQueryAdapter(queries.addRowIdColumn, errors, true).execute(globalState)) {
            throw new IgnoreMeException();
        }
        try {
            // Stamp identifiers once, in autocommit mode, before both runs: both then observe the same rows.
            if (!new SQLQueryAdapter(queries.stampRowIds, errors).execute(globalState)) {
                throw new IgnoreMeException();
            }
            List<List<String>> original = snapshotSide(globalState, queries.originalStatement, queries);
            List<List<String>> transformed = snapshotSide(globalState, queries.transformedStatement, queries);
            return new PostImages(original, transformed);
        } finally {
            new SQLQueryAdapter(queries.dropRowIdColumn, errors, true).execute(globalState);
        }
    }

    /**
     * Generates a DML statement of one kind together with its transformed counterpart. The kinds share this signature
     * so the oracle can pick one of them at random per check.
     *
     * @param <E>
     *            the DBMS-specific expression class
     * @param <T>
     *            the DBMS-specific table class
     * @param <C>
     *            the DBMS-specific column class
     */
    @FunctionalInterface
    private interface DMLStatementGenerator<E extends Expression<?>, T, C> {
        StatementPair<E> generate(T table, E predicate, E transformedPredicate,
                EETTransformer.TransformationRecord predicateRecord, List<C> orderByColumns, Integer limit);
    }

    /**
     * A DML statement and its transformed counterpart, which must leave the database in the same state, together with
     * the record of how the transformed one was built (so reduction can re-render it with fewer transformations).
     *
     * @param <E>
     *            the DBMS-specific expression class
     */
    private static final class StatementPair<E extends Expression<?>> {
        private final String original;
        private final String transformed;
        private final DMLTransformation<E> transformation;

        StatementPair(String original, String transformed, DMLTransformation<E> transformation) {
            this.original = original;
            this.transformed = transformed;
            this.transformation = transformation;
        }
    }

    /**
     * Generates an UPDATE and its transformed counterpart. Besides the WHERE predicate, UPDATE also transforms the
     * written values: each SET value expression is transformed in a scalar context. Its transformation sites are
     * ordered SET values first, then the predicate.
     *
     * @param table
     *            the table being modified
     * @param predicate
     *            the WHERE predicate of the original statement
     * @param transformedPredicate
     *            the transformed WHERE predicate, used by the transformed statement
     * @param predicateRecord
     *            the record of the predicate's transformation
     * @param orderByColumns
     *            the columns ordering the statement, empty if it is not capped by a limit
     * @param limit
     *            the maximum number of rows to modify, or {@code null} for no limit
     *
     * @return the original statement, its transformed counterpart and the latter's transformation record
     */
    private StatementPair<E> generateUpdateStatements(T table, E predicate, E transformedPredicate,
            EETTransformer.TransformationRecord predicateRecord, List<C> orderByColumns, Integer limit) {
        List<Map.Entry<C, E>> assignments = gen.generateSetAssignments();
        List<Map.Entry<C, E>> transformedAssignments = new ArrayList<>();
        List<E> expressions = new ArrayList<>();
        List<Boolean> booleanContexts = new ArrayList<>();
        List<EETTransformer.TransformationRecord> records = new ArrayList<>();
        List<C> assignmentColumns = new ArrayList<>();
        for (Map.Entry<C, E> assignment : assignments) {
            E transformedValue = transformer.transform(assignment.getValue(), false);
            transformedAssignments.add(new AbstractMap.SimpleEntry<>(assignment.getKey(), transformedValue));
            expressions.add(assignment.getValue());
            booleanContexts.add(false);
            records.add(transformer.getLastTransformationRecord());
            assignmentColumns.add(assignment.getKey());
        }
        expressions.add(predicate);
        booleanContexts.add(true);
        records.add(predicateRecord);
        DMLTransformation<E> transformation = new DMLTransformation<>(transformer, expressions, booleanContexts,
                records, replayed -> gen.updateStatement(table, zipAssignments(assignmentColumns, replayed),
                        replayed.get(replayed.size() - 1), orderByColumns, limit));
        return new StatementPair<>(gen.updateStatement(table, assignments, predicate, orderByColumns, limit),
                gen.updateStatement(table, transformedAssignments, transformedPredicate, orderByColumns, limit),
                transformation);
    }

    /**
     * Generates a DELETE and its transformed counterpart, which differ only in their WHERE predicate.
     *
     * @param table
     *            the table being modified
     * @param predicate
     *            the WHERE predicate of the original statement
     * @param transformedPredicate
     *            the transformed WHERE predicate, used by the transformed statement
     * @param predicateRecord
     *            the record of the predicate's transformation, which is DELETE's only transformation site
     * @param orderByColumns
     *            the columns ordering the statement, empty if it is not capped by a limit
     * @param limit
     *            the maximum number of rows to modify, or {@code null} for no limit
     *
     * @return the original statement, its transformed counterpart and the latter's transformation record
     */
    private StatementPair<E> generateDeleteStatements(T table, E predicate, E transformedPredicate,
            EETTransformer.TransformationRecord predicateRecord, List<C> orderByColumns, Integer limit) {
        DMLTransformation<E> transformation = new DMLTransformation<>(transformer, List.of(predicate), List.of(true),
                List.of(predicateRecord),
                replayed -> gen.deleteStatement(table, replayed.get(0), orderByColumns, limit));
        return new StatementPair<>(gen.deleteStatement(table, predicate, orderByColumns, limit),
                gen.deleteStatement(table, transformedPredicate, orderByColumns, limit), transformation);
    }

    /**
     * Generates an {@code INSERT ... SELECT} and its transformed counterpart. Besides the WHERE predicate, which
     * filters the source rows and is optional here, INSERT also transforms each inserted value in a scalar context. Its
     * transformation sites are ordered inserted values first, then the predicate when there is one.
     *
     * <p>
     * The ordering and limit cap the source rows the statement reads, so it inserts one row per source row kept.
     *
     * @param table
     *            the table being modified
     * @param predicate
     *            the WHERE predicate of the original statement
     * @param transformedPredicate
     *            the transformed WHERE predicate, used by the transformed statement
     * @param predicateRecord
     *            the record of the predicate's transformation, unused when no predicate is generated
     * @param orderByColumns
     *            the columns ordering the source rows, empty if the statement is not capped by a limit
     * @param limit
     *            the maximum number of source rows to insert from, or {@code null} for no limit
     *
     * @return the original statement, its transformed counterpart and the latter's transformation record
     */
    private StatementPair<E> generateInsertStatements(T table, E predicate, E transformedPredicate,
            EETTransformer.TransformationRecord predicateRecord, List<C> orderByColumns, Integer limit) {
        List<E> values = gen.generateInsertValues();
        List<E> transformedValues = new ArrayList<>();
        List<E> expressions = new ArrayList<>();
        List<Boolean> booleanContexts = new ArrayList<>();
        List<EETTransformer.TransformationRecord> records = new ArrayList<>();
        for (E value : values) {
            transformedValues.add(transformer.transform(value, false));
            expressions.add(value);
            booleanContexts.add(false);
            records.add(transformer.getLastTransformationRecord());
        }
        boolean withPredicate = Randomly.getBoolean();
        if (withPredicate) {
            expressions.add(predicate);
            booleanContexts.add(true);
            records.add(predicateRecord);
        }
        int valueCount = values.size();
        DMLTransformation<E> transformation = new DMLTransformation<>(transformer, expressions, booleanContexts,
                records, replayed -> gen.insertStatement(table, replayed.subList(0, valueCount),
                        withPredicate ? replayed.get(valueCount) : null, orderByColumns, limit));
        return new StatementPair<>(
                gen.insertStatement(table, values, withPredicate ? predicate : null, orderByColumns, limit),
                gen.insertStatement(table, transformedValues, withPredicate ? transformedPredicate : null,
                        orderByColumns, limit),
                transformation);
    }

    /**
     * Executes {@code statement} inside a transaction that is always rolled back, and returns the resulting post-image:
     * the surviving rows' identifier and content column values, ordered by identifier (the resulting database state). A
     * DBMS error the oracle tolerates aborts with {@link IgnoreMeException}; an oracle logic bug or unexpected error
     * surfaces as {@link AssertionError}.
     *
     * @param globalState
     *            the state whose connection the statement runs against
     * @param statement
     *            the DML statement to execute
     * @param queries
     *            supplies the transaction control and post-image select SQL and the post-image's column count
     *
     * @return the post-image, as one string list (identifier followed by content column values) per surviving row
     *
     * @throws SQLException
     *             if a DBMS interaction other than running {@code statement} fails; an error from {@code statement}
     *             itself instead surfaces as {@link IgnoreMeException} or {@link AssertionError}
     */
    private List<List<String>> snapshotSide(G globalState, String statement, ComparisonQueries queries)
            throws SQLException {
        new SQLQueryAdapter(queries.beginTransaction).execute(globalState);
        try {
            // execute reports (throws AssertionError for) unexpected errors and returns false for expected ones.
            boolean succeeded = new SQLQueryAdapter(statement, errors).execute(globalState);
            if (!succeeded) {
                // The statement hit an error the oracle tolerates; do not compare states (as EETOracle does for
                // SELECT).
                throw new IgnoreMeException();
            }
            return snapshotPostImage(globalState, queries.selectPostImage, queries.postImageColumns.size());
        } finally {
            new SQLQueryAdapter(queries.rollback).execute(globalState);
        }
    }

    /**
     * Reads the post-image produced by {@code selectStatement} into one string list per row (each column via
     * {@code getString}). A DBMS error the oracle tolerates aborts with {@link IgnoreMeException}; an oracle logic bug
     * or unexpected error surfaces as {@link AssertionError}.
     *
     * @param globalState
     *            the state whose connection the select runs against
     * @param selectStatement
     *            the post-image select to read; its columns are the identifier followed by the content columns
     * @param columnCount
     *            the number of columns to read from each row
     *
     * @return the read rows, in the select's order
     *
     * @throws SQLException
     *             if cleanup fails (errors thrown elsewhere will always be rethrown as {@link IgnoreMeException} or
     *             {@link AssertionError})
     */
    private List<List<String>> snapshotPostImage(G globalState, String selectStatement, int columnCount)
            throws SQLException {
        List<List<String>> rows = new ArrayList<>();
        SQLQueryAdapter q = new SQLQueryAdapter(selectStatement, errors, true,
                globalState.getOptions().canonicalizeSqlString());
        SQLancerResultSet result = null;
        try {
            result = q.executeAndGet(globalState);
            if (result == null) {
                throw new IgnoreMeException();
            }
            while (result.next()) {
                List<String> row = new ArrayList<>(columnCount);
                for (int i = 1; i <= columnCount; i++) {
                    row.add(result.getString(i));
                }
                rows.add(row);
            }
        } catch (Exception e) {
            if (e instanceof IgnoreMeException) {
                throw e;
            }
            Throwable current = e;
            while (current != null) {
                if (current.getMessage() != null && errors.errorIsExpected(current.getMessage())) {
                    throw new IgnoreMeException();
                }
                current = current.getCause();
            }
            throw new AssertionError(selectStatement, e);
        } finally {
            if (result != null && !result.isClosed()) {
                result.close();
            }
        }
        return rows;
    }

    private static String mismatchMessage(ComparisonQueries queries, PostImages images) {
        StringBuilder message = new StringBuilder()
                .append("-- The original and transformed statements left the database in different states.")
                .append(System.lineSeparator());
        renderStatementLines(message, queries);
        appendDiffRows(message, queries, images);
        return message.toString();
    }

    /**
     * Appends the post-image rows the two statements disagree on, pairing them up by row identifier so each line pair
     * shows one row as the original left it and as the transformed statement left it (or "(row absent)" where that side
     * does not have it at all). At most {@link #MAX_DIFF_ROWS_REPORTED} pairs are listed, as one mismatching statement
     * can differ in arbitrarily many rows and the point is to show what kind of difference it is.
     *
     * @param sb
     *            the builder to append to
     * @param queries
     *            the comparison the images came from, which names the post-image's columns
     * @param images
     *            the differing post-images
     */
    private static void appendDiffRows(StringBuilder sb, ComparisonQueries queries, PostImages images) {
        List<String> header = queries.postImageColumns;
        // Where the identifier sits within a post-image row, per the layout the generator defines
        int rowIdIndex = header.indexOf(EETDMLGenerator.ROW_ID_COLUMN);

        Map<String, List<String>> originalByRowId = indexByRowId(images.original, rowIdIndex);
        Map<String, List<String>> transformedByRowId = indexByRowId(images.transformed, rowIdIndex);
        Set<String> allRowIds = new TreeSet<>();
        allRowIds.addAll(originalByRowId.keySet());
        allRowIds.addAll(transformedByRowId.keySet());

        String nl = System.lineSeparator();
        sb.append("-- differing post-image rows (").append(String.join(", ", header)).append("):").append(nl);
        int shown = 0;
        for (String rowId : allRowIds) {
            List<String> originalRow = originalByRowId.get(rowId);
            List<String> transformedRow = transformedByRowId.get(rowId);
            if (Objects.equals(originalRow, transformedRow)) {
                continue;
            }
            if (shown == MAX_DIFF_ROWS_REPORTED) {
                sb.append("--   ... (further differences omitted)").append(nl);
                break;
            }
            sb.append("--   original:    ").append(renderRow(originalRow)).append(nl);
            sb.append("--   transformed: ").append(renderRow(transformedRow)).append(nl);
            shown++;
        }
    }

    // Pairs each assignment column with the correspondingly positioned (replayed) SET value expression. The values
    // list has one trailing element (the predicate) beyond the columns, which is left unpaired.
    private List<Map.Entry<C, E>> zipAssignments(List<C> columns, List<E> values) {
        List<Map.Entry<C, E>> assignments = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            assignments.add(new AbstractMap.SimpleEntry<>(columns.get(i), values.get(i)));
        }
        return assignments;
    }

    // Indexes a post-image by its row identifier, which each row holds at rowIdIndex
    private static Map<String, List<String>> indexByRowId(List<List<String>> image, int rowIdIndex) {
        Map<String, List<String>> byRowId = new LinkedHashMap<>();
        for (List<String> row : image) {
            byRowId.put(row.get(rowIdIndex), row);
        }
        return byRowId;
    }

    // Renders a post-image row for the finding message, or "(row absent)" when the row is missing on that side
    private static String renderRow(List<String> row) {
        return row == null ? "(row absent)" : row.toString();
    }

    @Override
    public String getLastQueryString() {
        return generatedQueryString;
    }

    @Override
    public Reproducer<G> getLastReproducer() {
        return reproducer;
    }
}
