package sqlancer.clickhouse.ast;

import sqlancer.clickhouse.ClickHouseSchema.ClickHouseTable;

public class ClickHouseTableReference extends ClickHouseExpression {

    private final ClickHouseTable table;

    public ClickHouseTableReference(ClickHouseTable table) {
        this.table = table;
    }

    public ClickHouseTable getTable() {
        return table;
    }

}
