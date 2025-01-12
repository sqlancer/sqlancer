package sqlancer.cockroachdb.ast;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.cockroachdb.CockroachDBVisitor;

public class CockroachDBConstant implements CockroachDBExpression {

    private CockroachDBConstant() {
    }

    // only for CODDTest. in comparison, the float value always has different precision
    public String toStringForComparison() {
        return toString();
    }

    public static class CockroachDBNullConstant extends CockroachDBConstant {

        @Override
        public String toString() {
            return "NULL";
        }

    }

    public static class CockroachDBIntConstant extends CockroachDBConstant {

        private final long value;

        public CockroachDBIntConstant(long value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }

        public long getValue() {
            return value;
        }

        @Override
        public String toStringForComparison() {
            BigDecimal decimal = new BigDecimal(value);
            return decimal.toPlainString();
        }

    }

    public static class CockroachDBBigDecimalConstant extends CockroachDBConstant {

        private final BigDecimal val;

        public CockroachDBBigDecimalConstant(BigDecimal val) {
            this.val = val;
        }

        public BigDecimal getValue() {
            return this.val;
        }

        @Override
        public String toString() {
            return String.valueOf(val);
        }

        @Override
        public String toStringForComparison() {
            String valString = val.toPlainString();
            if (valString.contains(".")){
                if (valString.indexOf(".") > 7) {
                    return valString.substring(0, valString.indexOf("."));
                }
                if (valString.length() < 5) {
                    if (valString.equals("0.0")) {
                        return "0";
                    }
                    else
                        return valString;
                }
                else {
                    return valString.substring(0, 5);
                }
            }
            else {
                return valString;
            }
        }
    }

    public static class CockroachDBDoubleConstant extends CockroachDBConstant {

        private final double value;

        public CockroachDBDoubleConstant(double value) {
            this.value = value;
        }

        public double getValue() {
            return value;
        }

        @Override
        public String toString() {
            if (value == Double.POSITIVE_INFINITY) {
                return "FLOAT '+Inf'";
            } else if (value == Double.NEGATIVE_INFINITY) {
                return "FLOAT '-Inf'";
            } else if (Double.isNaN(value)) {
                return "FLOAT 'NaN'";
            }
            return String.valueOf(value);
        }

        @Override
        public String toStringForComparison() {
            if (value == Double.POSITIVE_INFINITY) {
                return "FLOAT '+Inf'";
            } else if (value == Double.NEGATIVE_INFINITY) {
                return "FLOAT '-Inf'";
            } else if (Double.isNaN(value)) {
                return "Nan";
            }
            BigDecimal decimal = new BigDecimal(value);
            String stringDoubleValue = decimal.toPlainString();
            if (stringDoubleValue.contains(".") && stringDoubleValue.indexOf(".") > 7){
                return stringDoubleValue.substring(0, stringDoubleValue.indexOf("."));
            }
            if (!stringDoubleValue.contains(".")) {
                return stringDoubleValue;
            }
            if (stringDoubleValue.equals("0.0")) {
                return "0";
            }
            if (stringDoubleValue.length() < 5) {
                return stringDoubleValue;
            }
            else {
                return stringDoubleValue.substring(0, 5);
            }
        }
    }

    public static class CockroachDBTextConstant extends CockroachDBConstant {

        private final String value;

        public CockroachDBTextConstant(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        @Override
        public String toString() {
            return "'" + value.replace("'", "''") + "'";
        }

    }

    public static class CockroachDBBitConstant extends CockroachDBConstant {

        private final String value;

        public CockroachDBBitConstant(long value) {
            this.value = Long.toBinaryString(value);
        }

        public CockroachDBBitConstant(String binaryString) {
            this.value = binaryString;
        }

        public String getValue() {
            return value;
        }

        @Override
        public String toString() {
            return "B'" + value + "'";
        }

    }

    public static class CockroachDBBooleanConstant extends CockroachDBConstant {

        private final boolean value;

        public CockroachDBBooleanConstant(boolean value) {
            this.value = value;
        }

        public boolean getValue() {
            return value;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }

    }

    public static class CockroachDBArrayConstant extends CockroachDBConstant {

        private final List<CockroachDBExpression> elements;

        public CockroachDBArrayConstant(List<CockroachDBExpression> elements) {
            this.elements = elements;
        }

        public List<CockroachDBExpression> getElements() {
            return elements;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("ARRAY[");
            for (int i = 0; i < elements.size(); i++) {
                if (i != 0) {
                    sb.append(", ");
                }
                sb.append(CockroachDBVisitor.asString(elements.get(i)));
            }
            sb.append("]");
            return sb.toString();
        }
    }

    public static class CockroachDBIntervalConstant extends CockroachDBConstant {

        private final long year;
        private final long month;
        private final long day;
        private final long hour;
        private final long minute;
        private final long second;

        public CockroachDBIntervalConstant(long year, long month, long day, long hour, long minute, long second) {
            this.year = year;
            this.month = month;
            this.day = day;
            this.hour = hour;
            this.minute = minute;
            this.second = second;
        }

        @Override
        public String toString() {
            return String.valueOf(String.format("(INTERVAL '%d year %d months %d days %d hours %d minutes %d seconds')",
                    year, month, day, hour, minute, second));
        }

    }

    public static class CockroachDBTimeRelatedConstant extends CockroachDBConstant {

        private final String textRepr;
        private final String typeRepresentation;

        public CockroachDBTimeRelatedConstant(String typeRepresentation, long val, String format) {
            this.typeRepresentation = typeRepresentation;
            Timestamp timestamp = new Timestamp(val);
            SimpleDateFormat dateFormat = new SimpleDateFormat(format);
            textRepr = dateFormat.format(timestamp);

        }

        @Override
        public String toString() {
            return String.format("%s '%s'", typeRepresentation, textRepr);
        }

    }

    public static CockroachDBTextConstant createStringConstant(String text) {
        return new CockroachDBTextConstant(text);
    }

    public static CockroachDBDoubleConstant createFloatConstant(double val) {
        return new CockroachDBDoubleConstant(val);
    }

    public static CockroachDBIntConstant createIntConstant(long val) {
        return new CockroachDBIntConstant(val);
    }


    public static CockroachDBBigDecimalConstant createDecimalConstant(BigDecimal val) {
        return new CockroachDBBigDecimalConstant(val);
    }

    public static CockroachDBNullConstant createNullConstant() {
        return new CockroachDBNullConstant();
    }

    public static CockroachDBConstant createBooleanConstant(boolean val) {
        return new CockroachDBBooleanConstant(val);
    }

    public static CockroachDBExpression createBitConstant(long integer) {
        return new CockroachDBBitConstant(integer);
    }

    public static CockroachDBExpression createBitConstantWithSize(int size) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < size; i++) {
            sb.append(Randomly.getBoolean() ? 0 : 1);
        }
        return new CockroachDBBitConstant(sb.toString());
    }

    public static CockroachDBExpression createTimestampConstant(long integer) {
        return new CockroachDBTimeRelatedConstant("TIMESTAMP", integer, "yyyy-MM-dd'T'HH:mm:ss");
    }

    public static CockroachDBExpression createTimeConstant(long integer) {
        return new CockroachDBTimeRelatedConstant("TIME", integer, "yyyy-MM-dd'T'HH:mm:ss");
    }

    public static CockroachDBExpression createTimetz(long integer) {
        return new CockroachDBTimeRelatedConstant("TIMETZ", integer, "yyyy-MM-dd'T'HH:mm:ss"); // TODO: support the
                                                                                               // complete format
    }

    public static CockroachDBExpression createTimestamptzConstant(long integer) {
        return new CockroachDBTimeRelatedConstant("TIMESTAMPTZ", integer, "yyyy-MM-dd'T'HH:mm:ss"); // TODO: support the
                                                                                                    // complete
        // format
    }

    public static CockroachDBExpression createIntervalConstant(long year, long month, long day, long hour, long minute,
            long second) {
        return new CockroachDBIntervalConstant(year, month, day, hour, minute, second);
    }

    public static CockroachDBExpression createArrayConstant(List<CockroachDBExpression> elements) {
        return new CockroachDBArrayConstant(elements);
    }

}
