package sqlancer.risingwave.gen;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.DBMSCommon;
import sqlancer.risingwave.RisingWaveOptions;
import sqlancer.risingwave.RisingWaveSchema.RisingWaveColumn;
import sqlancer.risingwave.RisingWaveSchema.RisingWaveTable;
import sqlancer.risingwave.RisingWaveToStringVisitor;
import sqlancer.risingwave.RisingWaveType;

public final class RisingWaveTableGenerator {
    private RisingWaveTableGenerator() {
    }

    public static List<RisingWaveTable> generate(Randomly random, RisingWaveOptions options) {
        options.validate();
        List<RisingWaveTable> tables = new ArrayList<>();
        int count = random.getInteger(options.minTables, options.maxTables + 1);
        for (int table = 0; table < count; table++) {
            List<RisingWaveColumn> columns = new ArrayList<>();
            int width = random.getInteger(options.minColumns, options.maxColumns + 1);
            // A common typed join domain, with independent nullability and frequencies in each table.
            RisingWaveColumn joinKey = new RisingWaveColumn(DBMSCommon.createColumnName(0), RisingWaveType.INT,
                    Randomly.getBoolean());
            columns.add(joinKey);
            for (int c = 1; c < width; c++) {
                columns.add(new RisingWaveColumn(DBMSCommon.createColumnName(c),
                        Randomly.fromOptions(RisingWaveType.values()), Randomly.getBoolean()));
            }
            tables.add(new RisingWaveTable("t" + table + "_" + random.getInteger(0, 10000), columns, joinKey));
        }
        return tables;
    }

    public static String create(RisingWaveTable table) {
        return "CREATE TABLE " + RisingWaveToStringVisitor.identifier(table.getName()) + " ("
                + table.getColumns().stream().map(c -> RisingWaveToStringVisitor.identifier(c.getName()) + " "
                        + c.getType().sql() + (c.isNullable() ? "" : " NOT NULL")).collect(Collectors.joining(", "))
                + ")";
    }
}
