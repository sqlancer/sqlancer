package sqlancer.clickhouse.ast;

import sqlancer.clickhouse.ClickHouseSchema.ClickHouseColumn;

public class ClickHouseColumnReference extends ClickHouseExpression {

    private final ClickHouseColumn column;

    public ClickHouseColumnReference(ClickHouseColumn column) {
        this.column = column;
    }

    public static ClickHouseColumnReference create(ClickHouseColumn column) {
        return new ClickHouseColumnReference(column);
    }

    public ClickHouseColumn getColumn() {
        return column;
    }

}
