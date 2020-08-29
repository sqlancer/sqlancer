package sqlancer.mariadb.ast;

public class MariaDBConstant extends MariaDBExpression {

    public static class MariaDBNullConstant extends MariaDBConstant {

        @Override
        public String toString() {
            return "NULL";
        }

    }

    public static class MariaDBIntConstant extends MariaDBConstant {

        private final long value;

        public MariaDBIntConstant(long value) {
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

    public static class MariaDBDoubleConstant extends MariaDBConstant {

        private final double value;

        public MariaDBDoubleConstant(double value) {
            this.value = value;
        }

        public double getValue() {
            return value;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }

    }

    public static class MariaDBTextConstant extends MariaDBConstant {

        private final String value;

        public MariaDBTextConstant(String value) {
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

    public static class MariaDBBooleanConstant extends MariaDBConstant {

        private final boolean value;

        public MariaDBBooleanConstant(boolean value) {
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

    public static MariaDBTextConstant createTextConstant(String text) {
        return new MariaDBTextConstant(text);
    }

    public static MariaDBIntConstant createIntConstant(long val) {
        return new MariaDBIntConstant(val);
    }

    public static MariaDBNullConstant createNullConstant() {
        return new MariaDBNullConstant();
    }

    public static MariaDBConstant createBooleanConstant(boolean val) {
        return new MariaDBBooleanConstant(val);
    }

}
