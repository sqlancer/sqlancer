package sqlancer.common.gen;

public abstract class AbstractDeleteGenerator extends AbstractGenerator {

    protected AbstractDeleteGenerator() {
    }

    /**
     * Appends {@code DELETE FROM <tableName>}.
     *
     * @param tableName
     *            the name of the table to delete from.
     */
    protected void appendDeleteFromTable(String tableName) {
        appendDeleteFromTable(tableName, false);
    }

    /**
     * Appends {@code DELETE FROM [ONLY ]<tableName>}.
     *
     * @param tableName
     *            the name of the table to delete from.
     * @param only
     *            whether to emit the {@code ONLY} keyword (used by some databases to restrict deletion to the named
     *            table rather than its inheritance descendants).
     */
    protected void appendDeleteFromTable(String tableName, boolean only) {
        sb.append("DELETE FROM ");
        if (only) {
            sb.append("ONLY ");
        }
        sb.append(tableName);
    }

    /**
     * Appends {@code  LIMIT <value>} (with a leading space).
     *
     * @param value
     *            the LIMIT value, e.g. an integer literal or already-rendered expression. Converted via
     *            {@link StringBuilder#append(Object)}.
     */
    protected void appendLimitClause(Object value) {
        sb.append(" LIMIT ");
        sb.append(value);
    }

    /**
     * Appends {@code  RETURNING <expression>} (with a leading space).
     *
     * @param expression
     *            the rendered RETURNING expression.
     */
    protected void appendReturningClause(String expression) {
        sb.append(" RETURNING ");
        sb.append(expression);
    }

}
