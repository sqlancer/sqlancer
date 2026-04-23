package sqlancer.common.gen;

import java.util.List;

import sqlancer.common.schema.AbstractTableColumn;

public abstract class AbstractTableGenerator<C extends AbstractTableColumn<?, ?>> extends AbstractGenerator {

    protected void appendCreateTable(String tableName) {
        appendCreateTable(tableName, false);
    }

    protected void appendCreateTable(String tableName, boolean ifNotExists) {
        sb.append("CREATE TABLE ");
        if (ifNotExists) {
            sb.append("IF NOT EXISTS ");
        }
        sb.append(tableName);
    }

    protected void appendColumnDefinitions(List<C> columns) {
        sb.append("(");
        appendColumnDefinitionList(columns);
        sb.append(")");
    }

    protected void appendColumnDefinitionList(List<C> columns) {
        for (int i = 0; i < columns.size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            appendColumnDefinition(columns.get(i));
        }
    }

    protected void appendColumnDefinition(C column) {
        sb.append(column.getName());
        sb.append(" ");
        sb.append(column.getType());
    }

}
