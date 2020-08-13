package sqlancer.duckdb.ast;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;

import sqlancer.common.ast.newast.Node;

public class DuckDBConstant implements Node<DuckDBExpression> {

    private DuckDBConstant() {
    }

    public static class DuckDBNullConstant extends DuckDBConstant {

        @Override
        public String toString() {
            return "NULL";
        }

    }

    public static class DuckDBIntConstant extends DuckDBConstant {

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

    public static class DuckDBDoubleConstant extends DuckDBConstant {

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

    public static class DuckDBTextConstant extends DuckDBConstant {

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

    public static class DuckDBBitConstant extends DuckDBConstant {

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

    public static class DuckDBDateConstant extends DuckDBConstant {

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

    public static class DuckDBTimestampConstant extends DuckDBConstant {

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

    public static class DuckDBBooleanConstant extends DuckDBConstant {

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

    public static Node<DuckDBExpression> createStringConstant(String text) {
        return new DuckDBTextConstant(text);
    }

    public static Node<DuckDBExpression> createFloatConstant(double val) {
        return new DuckDBDoubleConstant(val);
    }

    public static Node<DuckDBExpression> createIntConstant(long val) {
        return new DuckDBIntConstant(val);
    }

    public static Node<DuckDBExpression> createNullConstant() {
        return new DuckDBNullConstant();
    }

    public static Node<DuckDBExpression> createBooleanConstant(boolean val) {
        return new DuckDBBooleanConstant(val);
    }

    public static Node<DuckDBExpression> createDateConstant(long integer) {
        return new DuckDBDateConstant(integer);
    }

    public static Node<DuckDBExpression> createTimestampConstant(long integer) {
        return new DuckDBTimestampConstant(integer);
    }

}
