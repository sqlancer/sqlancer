package sqlancer.spark.ast;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

public abstract class SparkConstant implements SparkExpression {

    public boolean isNull() {
        return false;
    }

    public static class SparkNullConstant extends SparkConstant {

        @Override
        public boolean isNull() {
            return true;
        }

        @Override
        public String toString() {
            return "NULL";
        }
    }

    public static class SparkIntConstant extends SparkConstant {

        private final long value;

        public SparkIntConstant(long value) {
            this.value = value;
        }

        public long getValue() {
            return value;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }
    }

    public static class SparkDoubleConstant extends SparkConstant {

        private final double value;

        public SparkDoubleConstant(double value) {
            this.value = value;
        }

        public double getValue() {
            return value;
        }

        @Override
        public String toString() {
            if (value == Double.POSITIVE_INFINITY) {
                return "CAST('Infinity' AS DOUBLE)";
            } else if (value == Double.NEGATIVE_INFINITY) {
                return "CAST('-Infinity' AS DOUBLE)";
            } else if (Double.isNaN(value)) {
                return "CAST('NaN' AS DOUBLE)";
            }
            return String.valueOf(value);
        }
    }

    public static class SparkDecimalConstant extends SparkConstant {

        private final BigDecimal value;

        public SparkDecimalConstant(BigDecimal value) {
            this.value = value;
        }

        public BigDecimal getValue() {
            return value;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }
    }

    public static class SparkTimestampConstant extends SparkConstant {

        private final String textRepr;

        public SparkTimestampConstant(long value) {
            Timestamp timestamp = new Timestamp(value);
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); // Spark prefers full timestamp
            this.textRepr = dateFormat.format(timestamp);
        }

        public String getValue() {
            return textRepr;
        }

        @Override
        public String toString() {
            return String.format("TIMESTAMP '%s'", textRepr);
        }
    }

    public static class SparkDateConstant extends SparkConstant {

        private final String textRepr;

        public SparkDateConstant(long value) {
            Timestamp timestamp = new Timestamp(value);
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            this.textRepr = dateFormat.format(timestamp);
        }

        public String getValue() {
            return textRepr;
        }

        @Override
        public String toString() {
            return String.format("DATE '%s'", textRepr);
        }
    }

    public static class SparkStringConstant extends SparkConstant {

        private final String value;

        public SparkStringConstant(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        @Override
        public String toString() {
            return "'" + value.replace("'", "''").replace("\\", "\\\\") + "'";
        }
    }

    public static class SparkBooleanConstant extends SparkConstant {

        private final boolean value;

        public SparkBooleanConstant(boolean value) {
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

    public static SparkConstant createNullConstant() {
        return new SparkNullConstant();
    }

    public static SparkConstant createIntConstant(long value) {
        return new SparkIntConstant(value);
    }

    public static SparkConstant createDoubleConstant(double value) {
        return new SparkDoubleConstant(value);
    }

    public static SparkConstant createDecimalConstant(BigDecimal value) {
        return new SparkDecimalConstant(value);
    }

    public static SparkConstant createTimestampConstant(long value) {
        return new SparkTimestampConstant(value);
    }

    public static SparkConstant createDateConstant(long value) {
        return new SparkDateConstant(value);
    }

    public static SparkConstant createStringConstant(String value) {
        return new SparkStringConstant(value);
    }

    public static SparkConstant createBooleanConstant(boolean value) {
        return new SparkBooleanConstant(value);
    }
}