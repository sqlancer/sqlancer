package sqlancer.tidb.ast;

public class TiDBConstant implements TiDBExpression {

    private TiDBConstant() {
    }

    public static class TiDBNullConstant extends TiDBConstant {

        @Override
        public String toString() {
            return "NULL";
        }

    }

    public static class TiDBIntConstant extends TiDBConstant {

        private final long value;

        public TiDBIntConstant(long value) {
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

    public static class TiDBDoubleConstant extends TiDBConstant {

        private final double value;

        public TiDBDoubleConstant(double value) {
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

    public static class TiDBTextConstant extends TiDBConstant {

        private final String value;

        public TiDBTextConstant(String value) {
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

    public static class TiDBBitConstant extends TiDBConstant {

        private final String value;

        public TiDBBitConstant(long value) {
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

    public static class TiDBBooleanConstant extends TiDBConstant {

        private final boolean value;

        public TiDBBooleanConstant(boolean value) {
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

    public static TiDBTextConstant createStringConstant(String text) {
        return new TiDBTextConstant(text);
    }

    public static TiDBDoubleConstant createFloatConstant(double val) {
        return new TiDBDoubleConstant(val);
    }

    public static TiDBIntConstant createIntConstant(long val) {
        return new TiDBIntConstant(val);
    }

    public static TiDBNullConstant createNullConstant() {
        return new TiDBNullConstant();
    }

    public static TiDBConstant createBooleanConstant(boolean val) {
        return new TiDBBooleanConstant(val);
    }

}
