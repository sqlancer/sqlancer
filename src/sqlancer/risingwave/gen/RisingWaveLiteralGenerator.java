package sqlancer.risingwave.gen;

import java.util.ArrayList;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.risingwave.RisingWaveType;
import sqlancer.risingwave.ast.RisingWaveArray;
import sqlancer.risingwave.ast.RisingWaveConstant;
import sqlancer.risingwave.ast.RisingWaveExpression;
import sqlancer.risingwave.ast.RisingWaveRow;

final class RisingWaveLiteralGenerator {
    private RisingWaveLiteralGenerator() {
    }

    static RisingWaveExpression literal(RisingWaveType type) {
        if (Randomly.getBooleanWithSmallProbability()) {
            return new RisingWaveConstant(type, null);
        }
        if (type.struct() || type.array()) {
            return composite(type);
        }
        switch (type) {
        case REAL:
        case DOUBLE:
            return new RisingWaveConstant(type, floatingPoint(type));
        case BOOLEAN:
            return new RisingWaveConstant(type, Randomly.fromOptions("true", "false"));
        case INT:
        case BIGINT:
            return new RisingWaveConstant(type, Randomly.fromOptions("-1", "0", "1", "2", "10"));
        case SMALLINT:
            return new RisingWaveConstant(type, Randomly.fromOptions("-32768", "32767", "-1", "0", "1", "2"));
        case TIME:
            return new RisingWaveConstant(type, time());
        case DECIMAL:
            return new RisingWaveConstant(type, Randomly.fromOptions("-1.25", "0", "0.01", "100.5"));
        case TEXT:
            return new RisingWaveConstant(type, Randomly.fromOptions("", "same", "a'b", "雪", "abc "));
        case DATE:
            return new RisingWaveConstant(type, Randomly.fromOptions("1970-01-01", "2000-02-29", "2024-12-31"));
        case TIMESTAMP:
            return new RisingWaveConstant(type, Randomly.fromOptions("1970-01-01 00:00:00", "2000-02-29 12:00:00"));
        case INTERVAL:
            return new RisingWaveConstant(type, Randomly.fromOptions("0 seconds", "1 day", "-1 hour", "2 months"));
        default:
            throw new AssertionError(type);
        }
    }

    static RisingWaveExpression composite(RisingWaveType type) {
        return type.struct() ? rowLiteral(type) : arrayLiteral(type);
    }

    static String floatingPoint(RisingWaveType type) {
        // Let RisingWave parse the value directly as REAL or DOUBLE.
        return Randomly.fromOptions("0", "-0", "0.5", "-0.25", "0.1", "NaN", "Infinity", "-Infinity",
                type == RisingWaveType.REAL ? "3.4028235e38" : "1.7976931348623157e308",
                type == RisingWaveType.REAL ? "1.17549435e-38" : "2.2250738585072014e-308");
    }

    static String time() {
        return Randomly.fromOptions("00:00:00", "23:59:59.999999", "12:34:56.123456");
    }

    private static RisingWaveArray arrayLiteral(RisingWaveType type) {
        List<RisingWaveExpression> elements = new ArrayList<>();
        int size = Randomly.smallNumber();
        RisingWaveType element = type.elementType();
        for (int i = 0; i < size; i++) {
            if (!elements.isEmpty() && Randomly.getBooleanWithRatherLowProbability()) {
                elements.add(elements.get(0));
                continue;
            }
            String value = null;
            if (!Randomly.getBooleanWithRatherLowProbability()) {
                switch (element) {
                case INT:
                    value = Randomly.fromOptions("-2147483648", "2147483647", "0", "1", "-1",
                            Long.toString(Randomly.getNotCachedInteger(-100, 101)));
                    break;
                case BIGINT:
                    value = Randomly.fromOptions("-9223372036854775808", "9223372036854775807", "0", "1", "-1",
                            Long.toString(Randomly.getNotCachedInteger(-10000, 10001)));
                    break;
                case BOOLEAN:
                    value = Randomly.fromOptions("true", "false");
                    break;
                case TEXT:
                    value = Randomly.fromOptions("", "NULL", "a'b", "a,b", "{x}", "a\\b", "\"q\"", "雪", "same");
                    break;
                default:
                    throw new AssertionError(element);
                }
            }
            elements.add(new RisingWaveConstant(element, value));
        }
        return new RisingWaveArray(type, elements);
    }

    private static RisingWaveExpression rowLiteral(RisingWaveType type) {
        List<RisingWaveExpression> fields = new ArrayList<>();
        for (RisingWaveType.Field field : type.fields()) {
            String value = Randomly.getBooleanWithRatherLowProbability() ? null : field.type == RisingWaveType.TEXT
                    ? Randomly.fromOptions("", "NULL", "a,b()\"\\雪", "a'b")
                    : field.type == RisingWaveType.BOOLEAN ? Randomly.fromOptions("true", "false")
                            : field.type == RisingWaveType.BIGINT
                                    ? Randomly.fromOptions("-9223372036854775808", "9223372036854775807", "1", "0")
                                    : Randomly.fromOptions("-2147483648", "2147483647", "1", "0");
            fields.add(new RisingWaveConstant(field.type, value));
        }
        return new RisingWaveRow(type, fields);
    }
}
