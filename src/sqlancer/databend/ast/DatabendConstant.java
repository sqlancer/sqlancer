package sqlancer.databend.ast;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;

import sqlancer.common.ast.newast.Node;

public class DatabendConstant implements Node<DatabendExpression> {

    private DatabendConstant() {
    }

    public static class DatabendNullConstant extends DatabendConstant {

        @Override
        public String toString() {
            return "NULL";
        }

    }

    public static class DatabendIntConstant extends DatabendConstant {

        private final long value;

        public DatabendIntConstant(long value) {
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

    public static class DatabendDoubleConstant extends DatabendConstant {

        private final double value;

        public DatabendDoubleConstant(double value) {
            this.value = value;
        }

        public double getValue() {
            return value;
        }

        @Override
        public String toString() {
            if (value == Double.POSITIVE_INFINITY) {
                return "3.40282347e+38";
            } else if (value == Double.NEGATIVE_INFINITY) {
                return "-3.40282347e+38";
            }

            return String.valueOf(value);
        }

    }

    public static class DatabendTextConstant extends DatabendConstant {

        private final String value;

        public DatabendTextConstant(String value) {
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

    public static class DatabendBitConstant extends DatabendConstant {

        private final String value;

        public DatabendBitConstant(long value) {
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

    public static class DatabendDateConstant extends DatabendConstant {

        public String textRepr;

        public DatabendDateConstant(long val) {
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

    public static class DatabendTimestampConstant extends DatabendConstant {

        public String textRepr;

        public DatabendTimestampConstant(long val) {
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

    public static class DatabendBooleanConstant extends DatabendConstant {

        private final boolean value;

        public DatabendBooleanConstant(boolean value) {
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

    public static Node<DatabendExpression> createStringConstant(String text) {
        return new DatabendTextConstant(text);
    }

    public static Node<DatabendExpression> createFloatConstant(double val) {
        return new DatabendDoubleConstant(val);
    }

    public static Node<DatabendExpression> createIntConstant(long val) {
        return new DatabendIntConstant(val);
    }

    public static Node<DatabendExpression> createNullConstant() {
        return new DatabendNullConstant();
    }

    public static Node<DatabendExpression> createBooleanConstant(boolean val) {
        return new DatabendBooleanConstant(val);
    }

    public static Node<DatabendExpression> createDateConstant(long integer) {
        return new DatabendDateConstant(integer);
    }

    public static Node<DatabendExpression> createTimestampConstant(long integer) {
        return new DatabendTimestampConstant(integer);
    }

}
