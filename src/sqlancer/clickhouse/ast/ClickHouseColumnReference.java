package sqlancer.clickhouse.ast;

import sqlancer.clickhouse.ClickHouseSchema.ClickHouseColumn;

public class ClickHouseColumnReference extends ClickHouseExpression {

    private final ClickHouseColumn column;
    private final ClickHouseConstant value;

    public ClickHouseColumnReference(ClickHouseColumn column, ClickHouseConstant value) {
        this.column = column;
        this.value = value;
    }

    public static ClickHouseColumnReference create(ClickHouseColumn column, ClickHouseConstant value) {
        return new ClickHouseColumnReference(column, value);
    }

    public ClickHouseColumn getColumn() {
        return column;
    }

    public ClickHouseConstant getValue() {
        return value;
    }

    @Override
    public ClickHouseConstant getExpectedValue() {
        return value;
    }

}
