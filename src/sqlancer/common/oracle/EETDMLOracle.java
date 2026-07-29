package sqlancer.common.oracle;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import sqlancer.ComparatorHelper;
import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.SQLGlobalState;
import sqlancer.common.ast.newast.Expression;
import sqlancer.common.gen.EETDMLGenerator;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
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
 * statements can be compared against the same starting state without permanently modifying the database. For a DELETE,
 * the state is captured as the set of surviving row identifiers. Because rolling back a statement requires a
 * transactional storage engine, the DBMS-specific setup must ensure only such engines are used while this oracle is
 * active.
 *
 * <p>
 * Only DELETE is currently supported. Statement reduction is not yet implemented (there is no
 * {@link sqlancer.Reproducer Reproducer}), so the finding is reported without database reduction.
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
        // DELETE targets a single table, so operate on exactly one; confining the generator to it keeps the predicate
        // from referencing another table's columns (which would render invalid single-table DML).
        T table = Randomly.fromList(tables);
        gen = gen.setTablesAndColumns(new AbstractTables<>(List.of(table)));

        E predicate = gen.generateBooleanExpression();
        // The WHERE predicate is evaluated in a boolean context.
        E transformedPredicate = transformer.transform(predicate, true);

        String originalDelete = gen.deleteStatement(table, predicate);
        String transformedDelete = gen.deleteStatement(table, transformedPredicate);
        generatedQueryString = originalDelete;

        // Add the auxiliary column outside the try, then guard everything after it with the finally that drops it:
        // the ALTER auto-commits (it is not undone by ROLLBACK), so a failure between adding and dropping would leak
        // the column into the next iteration and cause cascading duplicate-column failures.
        new SQLQueryAdapter(gen.addRowIdColumnStatement(table), true).execute(state);
        try {
            // Stamp identifiers once, in autocommit mode, before both DELETEs run: both then observe the same rows.
            new SQLQueryAdapter(gen.stampRowIdsStatement(table)).execute(state);

            Set<String> originalSurvivors = executeDeleteAndSnapshot(table, originalDelete);
            Set<String> transformedSurvivors = executeDeleteAndSnapshot(table, transformedDelete);

            if (!originalSurvivors.equals(transformedSurvivors)) {
                throw new AssertionError(
                        mismatchMessage(originalDelete, transformedDelete, originalSurvivors, transformedSurvivors));
            }
        } finally {
            new SQLQueryAdapter(gen.dropRowIdColumnStatement(table), true).execute(state);
        }
    }

    /**
     * Executes {@code deleteStatement} inside a transaction that is always rolled back, and returns the set of row
     * identifiers surviving the DELETE (the resulting database state). A DBMS error expected by the oracle aborts the
     * whole check ({@link IgnoreMeException}) rather than being reported, matching {@link EETOracle}'s handling; an
     * unexpected error surfaces as a bug ({@link AssertionError}, thrown by the query adapter).
     *
     * @param table
     *            the table being deleted from
     * @param deleteStatement
     *            the DELETE statement to execute
     *
     * @return the set of row identifiers surviving the DELETE
     *
     * @throws SQLException
     *             if a DBMS interaction fails
     */
    private Set<String> executeDeleteAndSnapshot(T table, String deleteStatement) throws SQLException {
        new SQLQueryAdapter(gen.beginTransactionStatement()).execute(state);
        try {
            // execute reports (throws AssertionError for) unexpected errors and returns false for expected ones.
            boolean succeeded = new SQLQueryAdapter(deleteStatement, errors).execute(state);
            if (!succeeded) {
                // The DELETE hit an error the oracle tolerates; do not compare states (as EETOracle does for SELECT).
                throw new IgnoreMeException();
            }
            return new HashSet<>(
                    ComparatorHelper.getResultSetFirstColumnAsString(gen.selectRowIdsStatement(table), errors, state));
        } finally {
            new SQLQueryAdapter(gen.rollbackTransactionStatement()).execute(state);
        }
    }

    private static String mismatchMessage(String originalDelete, String transformedDelete,
            Set<String> originalSurvivors, Set<String> transformedSurvivors) {
        return new StringBuilder()
                .append("-- The original and transformed DELETE statements left the database in different states")
                .append(" (different sets of surviving rows):").append(System.lineSeparator()).append("-- original (")
                .append(originalSurvivors.size()).append(" rows survive): ").append(originalDelete).append(';')
                .append(System.lineSeparator()).append("-- transformed (").append(transformedSurvivors.size())
                .append(" rows survive): ").append(transformedDelete).append(';').append(System.lineSeparator())
                .toString();
    }

    @Override
    public String getLastQueryString() {
        return generatedQueryString;
    }
}
