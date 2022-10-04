package sqlancer.yugabyte.ycql.ast;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;

import sqlancer.common.ast.newast.Node;

public class YCQLConstant implements Node<YCQLExpression> {

    private YCQLConstant() {
    }

    public static class YCQLNullConstant extends YCQLConstant {

        @Override
        public String toString() {
            return "NULL";
        }

    }

    public static class YCQLIntConstant extends YCQLConstant {

        private final long value;

        public YCQLIntConstant(long value) {
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

    public static class YCQLDoubleConstant extends YCQLConstant {

        private final double value;

        public YCQLDoubleConstant(double value) {
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

    public static class YCQLTextConstant extends YCQLConstant {

        private final String value;

        public YCQLTextConstant(String value) {
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

    public static class YCQLDateConstant extends YCQLConstant {

        public String textRepr;

        public YCQLDateConstant(long val) {
            Timestamp timestamp = new Timestamp(val);
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            textRepr = dateFormat.format(timestamp);
        }

        public String getValue() {
            return textRepr;
        }

        @Override
        public String toString() {
            return String.format("'%s'", textRepr);
        }

    }

    public static class YCQLTimestampConstant extends YCQLConstant {

        public String textRepr;

        public YCQLTimestampConstant(long val) {
            Timestamp timestamp = new Timestamp(val);
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            textRepr = dateFormat.format(timestamp);
        }

        public String getValue() {
            return textRepr;
        }

        @Override
        public String toString() {
            return String.format("'%s'", textRepr);
        }

    }

    public static class YCQLBooleanConstant extends YCQLConstant {

        private final boolean value;

        public YCQLBooleanConstant(boolean value) {
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

    public static Node<YCQLExpression> createStringConstant(String text) {
        return new YCQLTextConstant(text);
    }

    public static Node<YCQLExpression> createFloatConstant(double val) {
        return new YCQLDoubleConstant(val);
    }

    public static Node<YCQLExpression> createIntConstant(long val) {
        return new YCQLIntConstant(val);
    }

    public static Node<YCQLExpression> createNullConstant() {
        return new YCQLNullConstant();
    }

    public static Node<YCQLExpression> createBooleanConstant(boolean val) {
        return new YCQLBooleanConstant(val);
    }

    public static Node<YCQLExpression> createDateConstant(long integer) {
        return new YCQLDateConstant(integer);
    }

    public static Node<YCQLExpression> createTimestampConstant(long integer) {
        return new YCQLTimestampConstant(integer);
    }

}
