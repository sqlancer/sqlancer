package sqlancer.clickhouse.ast;

import sqlancer.clickhouse.ClickHouseSchema.ClickHouseColumn;

public class ClickHouseColumnReference extends ClickHouseExpression {

    private final ClickHouseColumn column;
    private final String columnAlias;
    private final String tableAlias;

    public ClickHouseColumnReference(ClickHouseColumn column, String columnAlias, String tableAlias) {
        this.column = column;
        this.columnAlias = columnAlias;
        this.tableAlias = tableAlias;
    }

    public ClickHouseColumnReference(ClickHouseAliasOperation alias) {
        this.column = new ClickHouseColumn(alias.getAlias(), null, true, false, null);
        this.columnAlias = null;
        this.tableAlias = null;
    }

    public ClickHouseColumn getColumn() {
        return column;
    }

    public String getAlias() {
        return columnAlias;
    }

    public String getTableAlias() {
        return tableAlias;
    }
}
