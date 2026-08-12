package sqlancer.common.gen;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
 * produced. The resulting state is compared as a full post-image (each surviving row's identifier and content column
 * values), which covers every DML statement: a DELETE removes rows from it, an UPDATE changes values in it.
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
     * Generates a fresh set of {@code column = value} assignments over the current tables' columns, used as an UPDATE
     * statement's SET clause. The columns are a random non-empty subset and each value is a fresh random expression;
     * both the columns and their assigned expressions are transformed by the oracle.
     *
     * @return the assignments, as {@code (column, value expression)} pairs (at least one)
     */
    List<Map.Entry<C, E>> generateSetAssignments();

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
     * column. For example, a 36-character UUID string.
     *
     * @param table
     *            the table whose rows are stamped
     *
     * @return the SQL statement
     */
    String stampRowIdsStatement(T table);

    /**
     * The SQL type of the auxiliary {@link #ROW_ID_COLUMN} column. It must be able to hold the identifiers that
     * {@link #stampRowIdsStatement} produces, so it belongs with that statement as the other half of the row-id
     * representation. For example, {@code VARCHAR(36)} would fit a 36-character UUID string.
     *
     * @return the column type
     */
    String rowIdColumnType();

    // --- Standard-SQL statements (override only where the DBMS's dialect differs) ---

    /**
     * SQL that adds the auxiliary {@link #ROW_ID_COLUMN} column to {@code table}, typed as {@link #rowIdColumnType}.
     *
     * @param table
     *            the table to add the column to
     *
     * @return the SQL statement
     */
    default String addRowIdColumnStatement(T table) {
        return "ALTER TABLE " + table.getName() + " ADD COLUMN " + ROW_ID_COLUMN + " " + rowIdColumnType();
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
     * SQL that reads back the full post-image of {@code table}: the {@link #ROW_ID_COLUMN} identifier and every content
     * column of every surviving row, ordered by the (unique) identifier so the two statements' snapshots align
     * row-for-row.
     *
     * <p>
     * This single value-level snapshot is the comparison surface for all DML statements: a DELETE removes rows from it,
     * an UPDATE changes column values in it. Row identity alone (which the identifier already captures) would suffice
     * for DELETE, but not for UPDATE, where the two runs could touch the same rows yet write different values.
     *
     * @param table
     *            the table to snapshot
     *
     * @return the SQL statement; its result columns are those of {@link #postImageColumns}, in that order
     */
    default String selectPostImageStatement(T table) {
        return "SELECT " + String.join(", ", postImageColumns(table)) + " FROM " + table.getName() + " ORDER BY "
                + ROW_ID_COLUMN;
    }

    /**
     * The columns a post-image row consists of, in the order {@link #selectPostImageStatement} returns them: the
     * {@link #ROW_ID_COLUMN} identifier followed by {@code table}'s content columns. This is the sole definition of the
     * post-image layout, so a consumer can find the identifier's position by looking up {@link #ROW_ID_COLUMN} here
     * rather than assuming one.
     *
     * @param table
     *            the table being snapshot
     *
     * @return the post-image column names, in order
     */
    default List<String> postImageColumns(T table) {
        List<String> columns = new ArrayList<>();
        columns.add(ROW_ID_COLUMN);
        for (C column : table.getColumns()) {
            columns.add(column.getName());
        }
        return columns;
    }

    /**
     * SQL that deletes the rows of {@code table} matching {@code predicate}, optionally limited to the first
     * {@code limit} rows (see {@link #orderByLimitClause}).
     *
     * @param table
     *            the table to delete from
     * @param predicate
     *            the WHERE predicate; rendered via {@link #asString}
     * @param orderByColumns
     *            the columns to order by before the row-id tiebreaker (may be empty); only used when {@code limit} is
     *            non-null
     * @param limit
     *            the maximum number of rows to delete, or {@code null} for no limit
     *
     * @return the SQL statement
     */
    default String deleteStatement(T table, E predicate, List<C> orderByColumns, Integer limit) {
        return "DELETE FROM " + table.getName() + " WHERE " + asString(predicate)
                + orderByLimitClause(orderByColumns, limit);
    }

    /**
     * SQL that updates the rows of {@code table} matching {@code predicate}, setting each column in {@code assignments}
     * to its assigned value expression, optionally limited to the first {@code limit} rows (see
     * {@link #orderByLimitClause}).
     *
     * @param table
     *            the table to update
     * @param assignments
     *            the {@code (column, value expression)} pairs to assign; each value is rendered via {@link #asString}
     * @param predicate
     *            the WHERE predicate; rendered via {@link #asString}
     * @param orderByColumns
     *            the columns to order by before the row-id tiebreaker (may be empty); only used when {@code limit} is
     *            non-null
     * @param limit
     *            the maximum number of rows to update, or {@code null} for no limit
     *
     * @return the SQL statement
     */
    default String updateStatement(T table, List<Map.Entry<C, E>> assignments, E predicate, List<C> orderByColumns,
            Integer limit) {
        List<String> setClauses = new ArrayList<>();
        for (Map.Entry<C, E> assignment : assignments) {
            setClauses.add(assignment.getKey().getName() + " = " + asString(assignment.getValue()));
        }
        return "UPDATE " + table.getName() + " SET " + String.join(", ", setClauses) + " WHERE " + asString(predicate)
                + orderByLimitClause(orderByColumns, limit);
    }

    /**
     * Renders the trailing {@code ORDER BY ... LIMIT n} clause shared by {@link #deleteStatement} and
     * {@link #updateStatement}, or the empty string when {@code limit} is null.
     *
     * <p>
     * The rows are ordered by {@code orderByColumns} followed by {@link #ROW_ID_COLUMN} as a tiebreaker. Because the
     * identifiers are unique, this is always a total order (even when the ordering columns tie), so the "first
     * {@code limit}" rows are identical for the original and transformed statements. Varying the ordering columns
     * exercises more access paths than the row id alone would. The caller must pass the same {@code orderByColumns} and
     * {@code limit} to both statements; neither is transformed.
     *
     * @param orderByColumns
     *            the columns to order by before the row-id tiebreaker (may be empty)
     * @param limit
     *            the maximum number of rows, or {@code null} for no limit (yielding an empty clause)
     *
     * @return the {@code ORDER BY ... LIMIT n} clause, or the empty string when {@code limit} is null
     */
    default String orderByLimitClause(List<C> orderByColumns, Integer limit) {
        if (limit == null) {
            return "";
        }
        List<String> orderBy = new ArrayList<>();
        for (C column : orderByColumns) {
            orderBy.add(column.getName());
        }
        orderBy.add(ROW_ID_COLUMN); // unique tiebreaker: guarantees a total order regardless of the columns above
        return " ORDER BY " + String.join(", ", orderBy) + " LIMIT " + limit;
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
