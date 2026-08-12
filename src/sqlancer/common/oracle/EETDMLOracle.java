package sqlancer.common.oracle;

import java.sql.SQLException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.SQLGlobalState;
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
 * changes values in it (row identity alone would suffice for DELETE, but not for UPDATE, which also transforms the
 * written values). Because rolling back a statement requires a transactional storage engine, the DBMS-specific setup
 * must ensure only such engines are used while this oracle is active.
 *
 * <p>
 * DELETE and UPDATE are currently supported (one is chosen at random per check). Statement reduction is not yet
 * implemented (there is no {@link sqlancer.Reproducer Reproducer}), so the finding is reported without database
 * reduction.
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

        // Optionally cap the statement with a LIMIT. The limit and its ordering (a random column subset, made a total
        // order by the row-id tiebreaker) are decided once and applied identically to both statements, so the capped
        // row set is deterministic and equal across the runs while still exercising varied orderings.
        Integer limit = null;
        List<C> orderByColumns = List.of();
        if (Randomly.getBoolean()) {
            limit = (int) Randomly.getNotCachedInteger(0, 10);
            orderByColumns = Randomly.subset(table.getColumns());
        }

        String originalStatement;
        String transformedStatement;
        if (Randomly.getBoolean()) {
            // UPDATE also transforms the written values: each SET value expression is transformed in a scalar context.
            List<Map.Entry<C, E>> assignments = gen.generateSetAssignments();
            List<Map.Entry<C, E>> transformedAssignments = new ArrayList<>();
            for (Map.Entry<C, E> assignment : assignments) {
                E transformedValue = transformer.transform(assignment.getValue(), false);
                transformedAssignments.add(new AbstractMap.SimpleEntry<>(assignment.getKey(), transformedValue));
            }
            originalStatement = gen.updateStatement(table, assignments, predicate, orderByColumns, limit);
            transformedStatement = gen.updateStatement(table, transformedAssignments, transformedPredicate,
                    orderByColumns, limit);
        } else {
            originalStatement = gen.deleteStatement(table, predicate, orderByColumns, limit);
            transformedStatement = gen.deleteStatement(table, transformedPredicate, orderByColumns, limit);
        }
        generatedQueryString = originalStatement;

        int columnCount = gen.postImageColumns(table).size();

        // Add the auxiliary column outside the try, then guard everything after it with the finally that drops it: the
        // ALTER auto-commits (it is not undone by ROLLBACK), so a failure between adding and dropping would leak the
        // column and cause cascading duplicate-column failures
        if (!new SQLQueryAdapter(gen.addRowIdColumnStatement(table), errors, true).execute(state)) {
            throw new IgnoreMeException();
        }
        try {
            // Stamp identifiers once, in autocommit mode, before both runs: both then observe the same rows.
            if (!new SQLQueryAdapter(gen.stampRowIdsStatement(table), errors).execute(state)) {
                throw new IgnoreMeException();
            }

            List<List<String>> originalImage = executeAndSnapshotPostImage(table, originalStatement, columnCount);
            List<List<String>> transformedImage = executeAndSnapshotPostImage(table, transformedStatement, columnCount);

            if (!originalImage.equals(transformedImage)) {
                throw new AssertionError(mismatchMessage(table, originalStatement, transformedStatement, originalImage,
                        transformedImage));
            }
        } finally {
            new SQLQueryAdapter(gen.dropRowIdColumnStatement(table), errors, true).execute(state);
        }
    }

    /**
     * Executes {@code statement} inside a transaction that is always rolled back, and returns the resulting post-image:
     * the surviving rows' identifier and content column values, ordered by identifier (the resulting database state). A
     * DBMS error the oracle tolerates aborts with {@link IgnoreMeException}; an oracle logic bug or unexpected error
     * surfaces as {@link AssertionError}.
     *
     * @param table
     *            the table being modified
     * @param statement
     *            the DML statement to execute
     * @param columnCount
     *            the number of columns the post-image select returns (identifier plus content columns)
     *
     * @return the post-image, as one string list (identifier followed by content column values) per surviving row
     *
     * @throws SQLException
     *             if a DBMS interaction other than running {@code statement} fails; an error from {@code statement}
     *             itself instead surfaces as {@link IgnoreMeException} or {@link AssertionError}
     */
    private List<List<String>> executeAndSnapshotPostImage(T table, String statement, int columnCount)
            throws SQLException {
        new SQLQueryAdapter(gen.beginTransactionStatement()).execute(state);
        try {
            // execute reports (throws AssertionError for) unexpected errors and returns false for expected ones.
            boolean succeeded = new SQLQueryAdapter(statement, errors).execute(state);
            if (!succeeded) {
                // The statement hit an error the oracle tolerates; do not compare states (as EETOracle does for
                // SELECT).
                throw new IgnoreMeException();
            }
            return snapshotPostImage(gen.selectPostImageStatement(table), columnCount);
        } finally {
            new SQLQueryAdapter(gen.rollbackTransactionStatement()).execute(state);
        }
    }

    /**
     * Reads the post-image produced by {@code selectStatement} into one string list per row (each column via
     * {@code getString}). A DBMS error the oracle tolerates aborts with {@link IgnoreMeException}; an oracle logic bug
     * or unexpected error surfaces as {@link AssertionError}.
     *
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
    private List<List<String>> snapshotPostImage(String selectStatement, int columnCount) throws SQLException {
        List<List<String>> rows = new ArrayList<>();
        SQLQueryAdapter q = new SQLQueryAdapter(selectStatement, errors, true,
                state.getOptions().canonicalizeSqlString());
        SQLancerResultSet result = null;
        try {
            result = q.executeAndGet(state);
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

    private String mismatchMessage(T table, String originalStatement, String transformedStatement,
            List<List<String>> originalImage, List<List<String>> transformedImage) {
        List<String> header = gen.postImageColumns(table);
        // Where the identifier sits within a post-image row, per the layout the generator defines
        int rowIdIndex = header.indexOf(EETDMLGenerator.ROW_ID_COLUMN);

        Map<String, List<String>> originalByRowId = indexByRowId(originalImage, rowIdIndex);
        Map<String, List<String>> transformedByRowId = indexByRowId(transformedImage, rowIdIndex);
        Set<String> allRowIds = new TreeSet<>();
        allRowIds.addAll(originalByRowId.keySet());
        allRowIds.addAll(transformedByRowId.keySet());

        String nl = System.lineSeparator();
        StringBuilder message = new StringBuilder()
                .append("-- The original and transformed statements left the database in different states.").append(nl)
                .append("-- original:    ").append(originalStatement).append(';').append(nl).append("-- transformed: ")
                .append(transformedStatement).append(';').append(nl).append("-- differing post-image rows (")
                .append(String.join(", ", header)).append("):").append(nl);
        int shown = 0;
        for (String rowId : allRowIds) {
            List<String> originalRow = originalByRowId.get(rowId);
            List<String> transformedRow = transformedByRowId.get(rowId);
            if (Objects.equals(originalRow, transformedRow)) {
                continue;
            }
            if (shown == MAX_DIFF_ROWS_REPORTED) {
                message.append("--   ... (further differences omitted)").append(nl);
                break;
            }
            message.append("--   original:    ").append(renderRow(originalRow)).append(nl);
            message.append("--   transformed: ").append(renderRow(transformedRow)).append(nl);
            shown++;
        }
        return message.toString();
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
}
