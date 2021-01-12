package sqlancer.clickhouse.ast;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Optional;
import java.util.regex.Pattern;

import ru.yandex.clickhouse.domain.ClickHouseDataType;

public final class ClickHouseCast extends ClickHouseExpression {

    private static final double MAX_INT_FOR_WHICH_CONVERSION_TO_INT_IS_TRIED = Math.pow(2, 51 - 1) - 1;
    private static final double MIN_INT_FOR_WHICH_CONVERSION_TO_INT_IS_TRIED = -Math.pow(2, 51 - 1);

    private static final byte FILE_SEPARATOR = 0x1c;
    private static final byte GROUP_SEPARATOR = 0x1d;
    private static final byte RECORD_SEPARATOR = 0x1e;
    private static final byte UNIT_SEPARATOR = 0x1f;
    private static final byte SYNCHRONOUS_IDLE = 0x16;

    static Connection castDatabase;

    private ClickHouseCast() {
    }

    public static Optional<Boolean> isTrue(ClickHouseConstant value) {
        ClickHouseConstant numericValue;
        if (value.getDataType() == ClickHouseDataType.Nothing) {
            return Optional.empty();
        }
        if (value.getDataType() == ClickHouseDataType.String) {
            numericValue = castToNumeric(value);
        } else {
            numericValue = value;
        }
        assert numericValue.getDataType() != ClickHouseDataType.String : numericValue + "should have been converted";
        switch (numericValue.getDataType()) {
        case Int32:
            return Optional.of(numericValue.asInt() != 0);
        case Float64:
            double doubleVal = numericValue.asDouble();
            return Optional.of(doubleVal != 0 && !Double.isNaN(doubleVal));
        default:
            throw new AssertionError(numericValue);
        }
    }

    // SELECT CAST('-1.370998801E9' AS INTEGER) == -1
    public static ClickHouseConstant castToInt(ClickHouseConstant cons) {
        switch (cons.getDataType()) {
        case Nothing:
            return ClickHouseConstant.createNullConstant();
        case Int32:
            return cons;
        case Float64:
            return ClickHouseConstant.createInt32Constant((long) cons.asDouble());
        case String:
            String asString = cons.asString();
            while (startsWithWhitespace(asString)) {
                asString = asString.substring(1);
            }
            if (!asString.isEmpty() && unprintAbleCharThatLetsBecomeNumberZero(asString)) {
                return ClickHouseConstant.createInt32Constant(0);
            }
            for (int i = asString.length(); i >= 0; i--) {
                try {
                    String substring = asString.substring(0, i);
                    Pattern p = Pattern.compile("[+-]?\\d\\d*");
                    if (p.matcher(substring).matches()) {
                        BigDecimal bg = new BigDecimal(substring);
                        long result;
                        try {
                            result = bg.longValueExact();
                        } catch (ArithmeticException e) {
                            if (substring.startsWith("-")) {
                                result = Long.MIN_VALUE;
                            } else {
                                result = Long.MAX_VALUE;
                            }
                        }
                        return ClickHouseConstant.createInt32Constant(result);
                    }
                } catch (Exception e) {

                }
            }
            return ClickHouseConstant.createInt32Constant(0);
        default:
            throw new AssertionError();
        }

    }

    public static ClickHouseConstant castToReal(ClickHouseConstant cons) {
        ClickHouseConstant numericValue = castToNumeric(cons);
        if (numericValue.getDataType() == ClickHouseDataType.Int32) {
            return ClickHouseConstant.createFloat64Constant(numericValue.asInt());
        } else {
            return numericValue;
        }
    }

    public static ClickHouseConstant castToNumericNoNumAsRealZero(ClickHouseConstant value) {
        return convertInternal(value, false, true, true);
    }

    public static ClickHouseConstant castToNumericFromNumOperand(ClickHouseConstant value) {
        return convertInternal(value, false, false, false);
    }

    /*
     * Applies numeric affinity to a value.
     */
    public static ClickHouseConstant castToNumeric(ClickHouseConstant value) {
        return convertInternal(value, true, false, false);
    }

    private static ClickHouseConstant convertInternal(ClickHouseConstant value, boolean convertRealToInt,
            boolean noNumIsRealZero, boolean convertIntToReal) throws AssertionError {
        switch (value.getDataType()) {
        case Nothing:
            return ClickHouseConstant.createNullConstant();
        case Int32:
        case Float64:
            return value;
        case String:
            String asString = value.asString();
            while (startsWithWhitespace(asString)) {
                asString = asString.substring(1);
            }
            if (!asString.isEmpty() && unprintAbleCharThatLetsBecomeNumberZero(asString)) {
                return ClickHouseConstant.createInt32Constant(0);
            }
            if (asString.toLowerCase().startsWith("-infinity") || asString.toLowerCase().startsWith("infinity")
                    || asString.startsWith("NaN")) {
                return ClickHouseConstant.createInt32Constant(0);
            }
            for (int i = asString.length(); i >= 0; i--) {
                try {
                    String substring = asString.substring(0, i);
                    double d = Double.parseDouble(substring);
                    BigDecimal first = new BigDecimal(substring);
                    long longValue = first.longValue();
                    BigDecimal second = BigDecimal.valueOf(longValue);
                    boolean isWithinConvertibleRange = longValue >= MIN_INT_FOR_WHICH_CONVERSION_TO_INT_IS_TRIED
                            && longValue <= MAX_INT_FOR_WHICH_CONVERSION_TO_INT_IS_TRIED && convertRealToInt;
                    boolean isFloatingPointNumber = substring.contains(".") || substring.toUpperCase().contains("E");
                    boolean doubleShouldBeConvertedToInt = isFloatingPointNumber && first.compareTo(second) == 0
                            && isWithinConvertibleRange;
                    boolean isInteger = !isFloatingPointNumber && first.compareTo(second) == 0;
                    if (doubleShouldBeConvertedToInt || isInteger && !convertIntToReal) {
                        // see https://www.sqlite.org/src/tktview/afdc5a29dc
                        return ClickHouseConstant.createInt32Constant(first.longValue());
                    } else {
                        return ClickHouseConstant.createFloat64Constant(d);
                    }
                } catch (Exception e) {
                }
            }
            if (noNumIsRealZero) {
                return ClickHouseConstant.createFloat64Constant(0.0);
            } else {
                return ClickHouseConstant.createInt32Constant(0);
            }
        default:
            throw new AssertionError(value);
        }
    }

    private static boolean startsWithWhitespace(String asString) {
        if (asString.isEmpty()) {
            return false;
        }
        char c = asString.charAt(0);
        switch (c) {
        case ' ':
        case '\t':
        case 0x0b:
        case '\f':
        case '\n':
        case '\r':
            return true;
        default:
            return false;
        }
    }

    private static boolean unprintAbleCharThatLetsBecomeNumberZero(String s) {
        // non-printable characters are ignored by Double.valueOf
        for (int i = 0; i < s.length(); i++) {
            char charAt = s.charAt(i);
            if (!Character.isISOControl(charAt) && !Character.isWhitespace(charAt)) {
                return false;
            }
            switch (charAt) {
            case GROUP_SEPARATOR:
            case FILE_SEPARATOR:
            case RECORD_SEPARATOR:
            case UNIT_SEPARATOR:
            case SYNCHRONOUS_IDLE:
                return true;
            default:
                // fall through
            }

            if (Character.isWhitespace(charAt)) {
                continue;
            } else {
                return true;
            }
        }
        return false;
    }

    public static ClickHouseConstant castToText(ClickHouseConstant cons) {
        if (cons.getDataType() == ClickHouseDataType.String) {
            return cons;
        }
        if (cons.getDataType() == ClickHouseDataType.Nothing) {
            return cons;
        }
        if (cons.getDataType() == ClickHouseDataType.Float64) {
            if (cons.asDouble() == Double.POSITIVE_INFINITY) {
                return ClickHouseConstant.createStringConstant("Inf");
            } else if (cons.asDouble() == Double.NEGATIVE_INFINITY) {
                return ClickHouseConstant.createStringConstant("-Inf");
            }
            return castRealToText(cons);
        }
        if (cons.getDataType() == ClickHouseDataType.Int32) {
            return ClickHouseConstant.createStringConstant(String.valueOf(cons.asInt()));
        }
        return null;
    }

    private static synchronized ClickHouseConstant castRealToText(ClickHouseConstant cons) throws AssertionError {
        try (Statement s = castDatabase.createStatement()) {
            String castResult = s.executeQuery("SELECT CAST(" + cons.asDouble() + " AS TEXT)").getString(1);
            return ClickHouseConstant.createStringConstant(castResult);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    public static ClickHouseConstant asBoolean(ClickHouseConstant val) {
        Optional<Boolean> boolVal = isTrue(val);
        if (boolVal.isPresent()) {
            return ClickHouseConstant.createBoolean(boolVal.get());
        } else {
            return ClickHouseConstant.createNullConstant();
        }
    }

}
