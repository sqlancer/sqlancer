package sqlancer.stonedb.ast;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;

import sqlancer.common.ast.newast.Node;

public class StoneDBConstant implements Node<StoneDBExpression> {

    private StoneDBConstant() {
    }

    public static class DuckDBNullConstant extends StoneDBConstant {

        @Override
        public String toString() {
            return "NULL";
        }

    }

    public static class DuckDBIntConstant extends StoneDBConstant {

        private final long value;

        public DuckDBIntConstant(long value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }

        public long getValue() {
            return value;
        }

    }

    public static class DuckDBDoubleConstant extends StoneDBConstant {

        private final double value;

        public DuckDBDoubleConstant(double value) {
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

    public static class DuckDBTextConstant extends StoneDBConstant {

        private final String value;

        public DuckDBTextConstant(String value) {
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

    public static class DuckDBBitConstant extends StoneDBConstant {

        private final String value;

        public DuckDBBitConstant(long value) {
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

    public static class DuckDBDateConstant extends StoneDBConstant {

        public String textRepr;

        public DuckDBDateConstant(long val) {
            Timestamp timestamp = new Timestamp(val);
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

    public static class DuckDBTimestampConstant extends StoneDBConstant {

        public String textRepr;

        public DuckDBTimestampConstant(long val) {
            Timestamp timestamp = new Timestamp(val);
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

    public static class DuckDBBooleanConstant extends StoneDBConstant {

        private final boolean value;

        public DuckDBBooleanConstant(boolean value) {
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

    public static Node<StoneDBExpression> createStringConstant(String text) {
        return new DuckDBTextConstant(text);
    }

    public static Node<StoneDBExpression> createFloatConstant(double val) {
        return new DuckDBDoubleConstant(val);
    }

    public static Node<StoneDBExpression> createIntConstant(long val) {
        return new DuckDBIntConstant(val);
    }

    public static Node<StoneDBExpression> createNullConstant() {
        return new DuckDBNullConstant();
    }

    public static Node<StoneDBExpression> createBooleanConstant(boolean val) {
        return new DuckDBBooleanConstant(val);
    }

    public static Node<StoneDBExpression> createDateConstant(long integer) {
        return new DuckDBDateConstant(integer);
    }

    public static Node<StoneDBExpression> createTimestampConstant(long integer) {
        return new DuckDBTimestampConstant(integer);
    }

}
