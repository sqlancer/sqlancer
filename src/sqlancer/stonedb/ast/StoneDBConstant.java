package sqlancer.stonedb.ast;

import static sqlancer.stonedb.StoneDBBugs.bugNotReported1;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;

import sqlancer.common.ast.newast.Node;

public class StoneDBConstant implements Node<StoneDBExpression> {

    private StoneDBConstant() {
    }

    public static class StoneDBNullConstant extends StoneDBConstant {
        @Override
        public String toString() {
            return "NULL";
        }
    }

    public static Node<StoneDBExpression> createNullConstant() {
        return new StoneDBNullConstant();
    }

    public static class StoneDBIntConstant extends StoneDBConstant {
        private final Integer value;

        public StoneDBIntConstant(int value) {
            this.value = value;
        }

        @Override
        public String toString() {
            if (value.equals(Integer.MIN_VALUE)) {
                return "INT_NULL";
            }
            return String.valueOf(value);
        }
    }

    public static Node<StoneDBExpression> createIntConstant(int val) {
        return new StoneDBIntConstant(val);
    }

    public static class StoneDBBigIntConstant extends StoneDBConstant {
        private final Long value;

        public StoneDBBigIntConstant(long value) {
            this.value = value;
        }

        @Override
        public String toString() {
            // For INT type: -2147483648 is reserved to indicate INT_NULL in Tianmu engine, Minimum Value Signed start
            // from -2147483647
            // refer: https://stonedb.io/docs/SQL-reference/data-types
            if (value.equals(Long.MIN_VALUE)) {
                return "BIGINT_NULL";
            }
            return String.valueOf(value);
        }
    }

    public static class StoneDBDoubleConstant extends StoneDBConstant {

        private final Double value;

        public StoneDBDoubleConstant(double value) {
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

    public static Node<StoneDBExpression> createDoubleConstant(double val) {
        if (bugNotReported1 && val < 0.5 && val > 0) {
            return new StoneDBDoubleConstant(0.5);
        }
        return new StoneDBDoubleConstant(val);
    }

    public static class StoneDBTextConstant extends StoneDBConstant {

        private final String value;

        public StoneDBTextConstant(String value) {
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

    public static Node<StoneDBExpression> createTextConstant(String text) {
        return new StoneDBTextConstant(text);
    }

    public static class StoneDBBitConstant extends StoneDBConstant {

        private final String value;

        public StoneDBBitConstant(long value) {
            this.value = Long.toBinaryString(value);
        }

        public String getValue() {
            return value;
        }

        @Override
        public String toString() {
            return "B'" + value + "'";
        }

    }

    public static class StoneDBDateConstant extends StoneDBConstant {

        public String textRepr;

        public StoneDBDateConstant(long val) {
            long validValue = val;
            // 9999-12-31 23:59:59.000999
            if (validValue > 253402271999999L) {
                validValue = 253402271999999L;
            }
            // 1000-01-01 00:00:00.000000
            if (validValue < -30609820800000L) {
                validValue = -30609820800000L;
            }
            Timestamp timestamp = new Timestamp(validValue);
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            textRepr = dateFormat.format(timestamp);
        }

        public String getValue() {
            return textRepr;
        }

        @Override
        public String toString() {
            return String.format("DATE '%s'", textRepr);
        }

    }

    public static Node<StoneDBExpression> createDateConstant(long integer) {
        return new StoneDBDateConstant(integer);
    }

    public static class StoneDBTimestampConstant extends StoneDBConstant {

        public String textRepr;

        public StoneDBTimestampConstant(long val) {
            long validValue = val;
            // 9999-12-31 23:59:59.000999
            if (validValue > 253402271999999L) {
                validValue = 253402271999999L;
            }
            // 1000-01-01 00:00:00.000000
            if (validValue < -30609820800000L) {
                validValue = -30609820800000L;
            }
            Timestamp timestamp = new Timestamp(validValue);
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            textRepr = dateFormat.format(timestamp);
        }

        public String getValue() {
            return textRepr;
        }

        @Override
        public String toString() {
            return String.format("TIMESTAMP '%s'", textRepr);
        }

    }

    public static Node<StoneDBExpression> createTimestampConstant(long integer) {
        return new StoneDBTimestampConstant(integer);
    }

    public static class StoneDBBooleanConstant extends StoneDBConstant {

        private final boolean value;

        public StoneDBBooleanConstant(boolean value) {
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

    public static Node<StoneDBExpression> createBooleanConstant(boolean val) {
        return new StoneDBBooleanConstant(val);
    }

}
