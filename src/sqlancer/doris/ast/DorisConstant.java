package sqlancer.doris.ast;

import sqlancer.IgnoreMeException;
import sqlancer.common.ast.newast.Node;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;

public class DorisConstant implements Node<DorisExpression> {

    private DorisConstant() {
    }

    public static class DorisNullConstant extends DorisConstant {

        @Override
        public String toString() {
            return "NULL";
        }

    }

    public static class DorisIntConstant extends DorisConstant {

        private final long value;

        public DorisIntConstant(long value) {
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

    public static class DorisDoubleConstant extends DorisConstant {

        private final double value;

        public DorisDoubleConstant(double value) {
            this.value = value;
            if (Double.isInfinite(value) || Double.isNaN(value)) {
                throw new IgnoreMeException();
            }
        }

        public double getValue() {
            return value;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }

    }

    public static class DorisTextConstant extends DorisConstant {

        private final String value;

        public DorisTextConstant(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        @Override
        public String toString() {
            return value;
        }

    }

    public static class DorisBitConstant extends DorisConstant {

        private final String value;

        public DorisBitConstant(long value) {
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

    public static class DorisDateConstant extends DorisConstant {

        public String textRepr;

        public DorisDateConstant(long val) {
            Timestamp timestamp = new Timestamp(val);
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            textRepr = dateFormat.format(timestamp);
        }

        public String getValue() {
            return textRepr;
        }

        @Override
        public String toString() {
            return String.format("%s", textRepr);
        }

    }

    public static class DorisDatetimeConstant extends DorisConstant {

        public String textRepr;

        public DorisDatetimeConstant(long val) {
            Timestamp timestamp = new Timestamp(val);
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            textRepr = dateFormat.format(timestamp);
        }

        public DorisDatetimeConstant() {
            textRepr = "CURRENT_TIMESTAMP";
        }

        public String getValue() {
            return textRepr;
        }

        @Override
        public String toString() {
            return String.format("%s", textRepr);
        }

    }

    public static class DorisBooleanConstant extends DorisConstant {

        private final boolean value;

        public DorisBooleanConstant(boolean value) {
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

    public static Node<DorisExpression> createStringConstant(String text) {
        return new DorisTextConstant(text);
    }

    public static Node<DorisExpression> createFloatConstant(double val) {
        return new DorisDoubleConstant(val);
    }

    public static Node<DorisExpression> createIntConstant(long val) {
        return new DorisIntConstant(val);
    }

    public static Node<DorisExpression> createNullConstant() {
        return new DorisNullConstant();
    }

    public static Node<DorisExpression> createBooleanConstant(boolean val) {
        return new DorisBooleanConstant(val);
    }

    public static Node<DorisExpression> createDateConstant(long integer) {
        return new DorisDateConstant(integer);
    }

    public static Node<DorisExpression> createDatetimeConstant(long integer) {
        return new DorisDatetimeConstant(integer);
    }

    public static Node<DorisExpression> createDatetimeConstant() {
        // use CURRENT_TIMESTAMP
        return new DorisDatetimeConstant();
    }

}
