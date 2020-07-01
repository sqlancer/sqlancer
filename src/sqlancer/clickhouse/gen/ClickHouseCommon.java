package sqlancer.clickhouse.gen;

import sqlancer.clickhouse.ClickHouseSchema;
import sqlancer.clickhouse.ast.ClickHouseExpression;
import sqlancer.clickhouse.ast.ClickHouseTableReference;

import java.util.ArrayList;
import java.util.List;

public final class ClickHouseCommon {

    private ClickHouseCommon() {
    }

    public static String createColumnName(int nr) {
        return String.format("c%d", nr);
    }

    public static List<ClickHouseExpression> getTableRefs(List<ClickHouseSchema.ClickHouseTable> tables,
            ClickHouseSchema s) {
        List<ClickHouseExpression> tableRefs = new ArrayList<>();
        for (ClickHouseSchema.ClickHouseTable t : tables) {
            ClickHouseTableReference tableRef;
            tableRef = new ClickHouseTableReference(t);
            tableRefs.add(tableRef);
        }
        return tableRefs;
    }

}
