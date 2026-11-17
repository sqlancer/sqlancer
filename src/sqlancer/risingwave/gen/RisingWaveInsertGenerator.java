package sqlancer.risingwave.gen;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.risingwave.RisingWaveOptions;
import sqlancer.risingwave.RisingWaveSchema.RisingWaveColumn;
import sqlancer.risingwave.RisingWaveSchema.RisingWaveTable;
import sqlancer.risingwave.RisingWaveToStringVisitor;
import sqlancer.risingwave.RisingWaveType;
import sqlancer.risingwave.ast.RisingWaveConstant;
import sqlancer.risingwave.ast.RisingWaveExpression;

public final class RisingWaveInsertGenerator {
    private RisingWaveInsertGenerator() {
    }

    public static List<String> generate(Randomly random, RisingWaveOptions options, RisingWaveTable table, int index) {
        int rows = random.getInteger(options.minRows, options.maxRows + 1);
        int repeated = random.getInteger(0, 3);
        int frequency = random.getInteger(1, 5);
        RisingWaveColumn joinKey = table.getInitialJoinKeyColumn();
        if (joinKey == null) {
            throw new IllegalArgumentException("Initial-data table has no join key");
        }
        List<String> result = new ArrayList<>();
        for (int row = 0; row < rows; row++) {
            List<RisingWaveExpression> values = new ArrayList<>();
            for (RisingWaveColumn column : table.getColumns()) {
                values.add(initialValue(column, joinKey, random, row, rows, index, repeated, frequency));
            }
            result.add(render(table, values));
        }
        return result;
    }

    private static RisingWaveExpression initialValue(RisingWaveColumn column, RisingWaveColumn joinKey, Randomly random,
            int row, int rows, int tableIndex, int repeated, int frequency) {
        String value;
        if (column.isNullable() && (row == 0 || column != joinKey && Randomly.getBooleanWithSmallProbability())) {
            value = null;
        } else if (column == joinKey) {
            value = Integer.toString(
                    row == rows - 1 ? 1000 + tableIndex : row == 1 ? 1 : row % frequency == 0 ? repeated : row % 3);
        } else {
            if (column.getType().struct() || column.getType().array()) {
                return RisingWaveLiteralGenerator.composite(column.getType());
            }
            value = nonKeyValue(column.getType(), random, row);
        }
        return new RisingWaveConstant(column.getType(), value);
    }

    private static String render(RisingWaveTable table, List<RisingWaveExpression> values) {
        return "INSERT INTO " + RisingWaveToStringVisitor.identifier(table.getName()) + " VALUES ("
                + values.stream().map(RisingWaveToStringVisitor::asString).collect(Collectors.joining(", ")) + ")";
    }

    private static String nonKeyValue(RisingWaveType type, Randomly random, int row) {
        boolean edge = row % 3 != 2;
        switch (type) {
        case REAL:
        case DOUBLE:
            return edge ? RisingWaveLiteralGenerator.floatingPoint(type)
                    : random.getInteger(-100, 101) + Randomly.fromOptions(".125", ".1", ".5");
        case BOOLEAN:
            return Boolean.toString(Randomly.getBoolean());
        case SMALLINT:
            return edge ? Randomly.fromOptions("-32768", "32767", "0", "1", "-1")
                    : Integer.toString(random.getInteger(-32768, 32768));
        case TIME:
            return edge ? RisingWaveLiteralGenerator.time()
                    : String.format(java.util.Locale.ROOT, "%02d:%02d:%02d.%06d", random.getInteger(0, 24),
                            random.getInteger(0, 60), random.getInteger(0, 60), random.getInteger(0, 1000000));
        case INT:
            return edge ? Randomly.fromOptions("-2147483648", "2147483647", "0", "1")
                    : Integer.toString(random.getInteger(-100, 101));
        case BIGINT:
            return edge ? Randomly.fromOptions("-9223372036854775808", "9223372036854775807", "0", "1")
                    : Long.toString(random.getInteger(-10000, 10001));
        case DECIMAL:
            return edge ? Randomly.fromOptions("-999999999999.99", "999999999999.99", "0", "0.00001")
                    : random.getInteger(-100, 101) + ".25";
        case TEXT:
            return edge ? Randomly.fromOptions("", "same", "a'b", "雪", " ", "\\", "a\nb")
                    : "v" + random.getInteger(0, 10);
        case DATE:
            return edge ? Randomly.fromOptions("0001-01-01", "9999-12-31", "2000-02-29", "1970-01-01")
                    : String.format("2024-%02d-%02d", random.getInteger(1, 13), random.getInteger(1, 29));
        case TIMESTAMP:
            return edge ? Randomly.fromOptions("0001-01-01 00:00:00", "9999-12-31 23:59:59.999999",
                    "2000-02-29 12:00:00", "1970-01-01 00:00:00")
                    : String.format("2024-01-%02d 12:00:00", random.getInteger(1, 29));
        case INTERVAL:
            return Randomly.fromOptions("0 seconds", "-1 hour", "2 months", "10000 days", "1 second");
        default:
            throw new AssertionError(type);
        }
    }
}
