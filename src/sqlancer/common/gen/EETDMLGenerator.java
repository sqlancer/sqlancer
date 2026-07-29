package sqlancer.common.gen;

import sqlancer.common.ast.newast.Expression;
import sqlancer.common.oracle.EETTransformer;
import sqlancer.common.schema.AbstractTable;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.common.schema.AbstractTables;

/**
 * Generator interface used by {@link sqlancer.common.oracle.EETDMLOracle}, the DML counterpart of {@link EETGenerator}.
 * It supplies methods which generate the transformable expressions, create the DBMS-specific {@link EETTransformer}
 * that rewrites them, and produce the SQL of the statements the oracle uses to observe the database state a statement
 * produces (an approach drawn from the DQE oracle).
 *
 * <p>
 * Adapted from the DQE oracle, state is observed with an auxiliary column ({@link EETDMLGenerator#ROW_ID_COLUMN}) which
 * uniquely identifies each row. The rows are stamped with identifiers once, before both executions of the statement run
 * (each in a rolled-back transaction), so both executions observe the same identifiers regardless of how they are
 * produced.
 *
 * <p>
 * Most of these statements are standard SQL, likely common to most DBMSs, so are provided as {@code default} methods.
 *
 * @param <E>
 *            the DBMS-specific expression class
 * @param <T>
 *            the DBMS-specific table class
 * @param <C>
 *            the DBMS-specific column class
 */
public interface EETDMLGenerator<E extends Expression<C>, T extends AbstractTable<C, ?, ?>, C extends AbstractTableColumn<?, ?>> {

    /** Name of the auxiliary column that uniquely identifies each row. */
    String ROW_ID_COLUMN = "rowid";

    /**
     * Restricts this generator to the given tables (a single table, for the DML statement under test) and their
     * columns.
     *
     * @param tables
     *            the tables (and, implicitly, columns) the generated statement operates on
     *
     * @return this generator
     */
    EETDMLGenerator<E, T, C> setTablesAndColumns(AbstractTables<T, C> tables);

    /**
     * Generates a fresh random boolean expression over the current tables' columns, used as the DML statement's WHERE
     * predicate.
     *
     * @return a fresh random boolean expression
     */
    E generateBooleanExpression();

    /**
     * Creates a DBMS-specific {@link EETTransformer} backed by this generator, used to rewrite the statement's
     * expressions into semantically equivalent ones.
     *
     * @return a DBMS-specific {@link EETTransformer}
     */
    EETTransformer<E, ?> createTransformer();

    // --- DBMS-specific primitives ---

    /**
     * Renders an expression to its DBMS-specific SQL string.
     *
     * @param expr
     *            the expression to render
     *
     * @return the SQL text of {@code expr}
     */
    String asString(E expr);

    /**
     * SQL that assigns every existing row of {@code table} a distinct, stable identifier in the {@link #ROW_ID_COLUMN}
     * column. DBMS-specific because it names the DBMS's UUID-generating function.
     *
     * @param table
     *            the table whose rows are stamped
     *
     * @return the SQL statement
     */
    String stampRowIdsStatement(T table);

    // --- Standard-SQL statements (override only where the DBMS's dialect differs) ---

    /**
     * SQL that adds the auxiliary {@link #ROW_ID_COLUMN} column to {@code table}.
     *
     * @param table
     *            the table to add the column to
     *
     * @return the SQL statement
     */
    default String addRowIdColumnStatement(T table) {
        return "ALTER TABLE " + table.getName() + " ADD COLUMN " + ROW_ID_COLUMN + " VARCHAR(36)";
    }

    /**
     * SQL that drops the auxiliary {@link #ROW_ID_COLUMN} column from {@code table}.
     *
     * @param table
     *            the table to drop the column from
     *
     * @return the SQL statement
     */
    default String dropRowIdColumnStatement(T table) {
        return "ALTER TABLE " + table.getName() + " DROP COLUMN " + ROW_ID_COLUMN;
    }

    /**
     * SQL that selects the {@link #ROW_ID_COLUMN} of every row of {@code table} (the surviving-row snapshot).
     *
     * @param table
     *            the table to snapshot
     *
     * @return the SQL statement; its first result column must be the identifiers
     */
    default String selectRowIdsStatement(T table) {
        return "SELECT " + ROW_ID_COLUMN + " FROM " + table.getName();
    }

    /**
     * SQL that deletes the rows of {@code table} matching {@code predicate}.
     *
     * @param table
     *            the table to delete from
     * @param predicate
     *            the WHERE predicate; rendered via {@link #asString}
     *
     * @return the SQL statement
     */
    default String deleteStatement(T table, E predicate) {
        return "DELETE FROM " + table.getName() + " WHERE " + asString(predicate);
    }

    /**
     * SQL that starts a transaction, so a statement's effect can be observed and then undone.
     *
     * @return the SQL statement
     */
    default String beginTransactionStatement() {
        return "BEGIN";
    }

    /**
     * SQL that rolls the current transaction back, undoing the statement's effect.
     *
     * @return the SQL statement
     */
    default String rollbackTransactionStatement() {
        return "ROLLBACK";
    }
}
