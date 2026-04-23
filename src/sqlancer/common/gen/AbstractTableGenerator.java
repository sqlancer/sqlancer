package sqlancer.common.gen;

import java.util.List;

import sqlancer.common.schema.AbstractTableColumn;

public abstract class AbstractTableGenerator<C extends AbstractTableColumn<?, ?>> extends AbstractGenerator {

    /** Appends {@code CREATE TABLE <name>}. */
    protected void appendCreateTable(String tableName) {
        appendCreateTable(tableName, false);
    }

    /** Appends {@code CREATE TABLE [IF NOT EXISTS ]<name>}. */
    protected void appendCreateTable(String tableName, boolean ifNotExists) {
        sb.append("CREATE TABLE ");
        if (ifNotExists) {
            sb.append("IF NOT EXISTS ");
        }
        sb.append(tableName);
    }

    /**
     * Appends a parenthesized, comma-separated column definition list, e.g. {@code (c0 INT, c1 TEXT)}. Delegates each
     * column's rendering to {@link #appendColumnDefinition(AbstractTableColumn)}.
     */
    protected void appendColumnDefinitions(List<C> columns) {
        sb.append("(");
        appendColumnDefinitionList(columns);
        sb.append(")");
    }

    /**
     * Appends a comma-separated column definition list without enclosing parentheses, e.g. {@code c0 INT, c1 TEXT}.
     * Useful when subclasses also emit table-level constraints (e.g. {@code PRIMARY KEY (...)}) inside the same parens.
     */
    protected void appendColumnDefinitionList(List<C> columns) {
        for (int i = 0; i < columns.size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            appendColumnDefinition(columns.get(i));
        }
    }

    /**
     * Appends a single column's definition. Default output is {@code <name> <type>}, e.g. {@code c0 INT}. Override to
     * add constraints such as {@code NOT NULL}, {@code DEFAULT ...}, or {@code CHECK (...)}.
     */
    protected void appendColumnDefinition(C column) {
        sb.append(column.getName());
        sb.append(" ");
        sb.append(column.getType());
    }

}
