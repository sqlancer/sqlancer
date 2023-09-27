package sqlancer.clickhouse.ast;

import java.util.List;
import java.util.stream.Collectors;

import sqlancer.clickhouse.ClickHouseSchema.ClickHouseTable;

public class ClickHouseTableReference extends ClickHouseExpression {

    private final ClickHouseTable table;
    private final String alias;

    public ClickHouseTableReference(ClickHouseTable table, String alias) {
        this.table = table;
        this.alias = alias;
    }

    public ClickHouseTable getTable() {
        return table;
    }

    public String getTableName() {
        return (alias == null) ? table.getName() : alias;
    }

    public String getAlias() {
        return alias;
    }

    public List<ClickHouseColumnReference> getColumnReferences() {
        return this.table.getColumns().stream().map(c -> c.asColumnReference(this.alias)).collect(Collectors.toList());
    }

}
