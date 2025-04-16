package sqlancer.hive.ast;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

public abstract class HiveConstant implements HiveExpression {

    public boolean isNull() {
        return false;
    }

    public static class HiveNullConstant extends HiveConstant {

        @Override
        public boolean isNull() {
            return true;
        }

        @Override
        public String toString() {
            return "NULL";
        }
    }

    public static class HiveIntConstant extends HiveConstant {

        private final long value;

        public HiveIntConstant(long value) {
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

    public static class HiveDoubleConstant extends HiveConstant {

        private final double value;

        public HiveDoubleConstant(double value) {
            this.value = value;
        }

        public double getValue() {
            return value;
        }

        @Override
        public String toString() {
            if (value == Double.POSITIVE_INFINITY) {
                return "'+Inf'";
            } else if (value == Double.NEGATIVE_INFINITY) {
                return "'-Inf'";
            }
            return String.valueOf(value);
        }
    }

    public static class HiveDecimalConstant extends HiveConstant {

        private final BigDecimal value;

        public HiveDecimalConstant(BigDecimal value) {
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

    public static class HiveTimestampConstant extends HiveConstant {

        private final String textRepr;

        public HiveTimestampConstant(long value) {
            Timestamp timestamp = new Timestamp(value);
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
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

    public static class HiveDateConstant extends HiveConstant {

        private final String textRepr;

        public HiveDateConstant(long value) {
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

    public static class StringConstant extends HiveConstant {

        private final String value;

        public StringConstant(String value) {
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

    public static class HiveBooleanConstant extends HiveConstant {

        private final boolean value;

        public HiveBooleanConstant(boolean value) {
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

    public static HiveConstant createNullConstant() {
        return new HiveNullConstant();
    }

    public static HiveConstant createIntConstant(long value) {
        return new HiveIntConstant(value);
    }

    public static HiveConstant createDoubleConstant(double value) {
        return new HiveDoubleConstant(value);
    }

    public static HiveConstant createDecimalConstant(BigDecimal value) {
        return new HiveDecimalConstant(value);
    }

    public static HiveConstant createTimestampConstant(long value) {
        return new HiveTimestampConstant(value);
    }

    public static HiveConstant createDateConstant(long value) {
        return new HiveDateConstant(value);
    }

    public static HiveConstant createStringConstant(String value) {
        return new StringConstant(value);
    }

    public static HiveConstant createBooleanConstant(boolean value) {
        return new HiveBooleanConstant(value);
    }
}
