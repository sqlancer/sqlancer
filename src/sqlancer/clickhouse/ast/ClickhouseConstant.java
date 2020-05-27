package sqlancer.clickhouse.ast;

import sqlancer.ast.newast.Node;

public class ClickhouseConstant implements Node<ClickhouseExpression> {

    private ClickhouseConstant() {
    }

    public static class ClickhouseNullConstant extends ClickhouseConstant {

        @Override
        public String toString() {
            return "NULL";
        }

    }

    public static class ClickhouseIntConstant extends ClickhouseConstant {

        private final long value;

        public ClickhouseIntConstant(long value) {
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

    public static class ClickhouseDoubleConstant extends ClickhouseConstant {

        private final double value;

        public ClickhouseDoubleConstant(double value) {
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

    public static class ClickhouseTextConstant extends ClickhouseConstant {

        private final String value;

        public ClickhouseTextConstant(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        @Override
        public String toString() {
            return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'";
        }

    }

    public static class ClickhouseBitConstant extends ClickhouseConstant {

        private final String value;

        public ClickhouseBitConstant(long value) {
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

    public static class ClickhouseBooleanConstant extends ClickhouseConstant {

        private final boolean value;

        public ClickhouseBooleanConstant(boolean value) {
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

    public static Node<ClickhouseExpression> createStringConstant(String text) {
        return new ClickhouseTextConstant(text);
    }

    public static Node<ClickhouseExpression> createFloatConstant(double val) {
        return new ClickhouseDoubleConstant(val);
    }

    public static Node<ClickhouseExpression> createIntConstant(long val) {
        return new ClickhouseIntConstant(val);
    }

    public static Node<ClickhouseExpression> createNullConstant() {
        return new ClickhouseNullConstant();
    }

    public static Node<ClickhouseExpression> createBooleanConstant(boolean val) {
        return new ClickhouseBooleanConstant(val);
    }

}
