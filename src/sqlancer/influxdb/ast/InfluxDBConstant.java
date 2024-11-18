package sqlancer.influxdb.ast;

public class InfluxDBConstant implements InfluxDBExpression {

    // Private constructor to prevent instantiation
    private InfluxDBConstant() {
    }

    // Represents a NULL constant
    public static class InfluxDBNullConstant extends InfluxDBConstant {
        @Override
        public String toString() {
            return "NULL";
        }
    }

    // Represents an integer constant
    public static class InfluxDBIntConstant extends InfluxDBConstant {
        private final long value;

        public InfluxDBIntConstant(long value) {
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

    // Represents a boolean constant
    public static class InfluxDBBooleanConstant extends InfluxDBConstant {
        private final boolean value;

        public InfluxDBBooleanConstant(boolean value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }

        public boolean getValue() {
            return value;
        }
    }

    // Represents a string constant
    public static class InfluxDBStringConstant extends InfluxDBConstant {
        private final String value;

        public InfluxDBStringConstant(String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return '"' + value + '"';
        }

        public String getValue() {
            return value;
        }
    }

    // Represents a double constant
    public static class InfluxDBDoubleConstant extends InfluxDBConstant {
        private final double value;

        public InfluxDBDoubleConstant(double value) {
            this.value = value;
        }

        @Override
        public String toString() {
            if (value == Double.POSITIVE_INFINITY) {
                return "cast('Infinity' as double)";
            } else if (value == Double.NEGATIVE_INFINITY) {
                return "cast('-Infinity' as double)";
            }
            return String.valueOf(value);
        }

        public double getValue() {
            return value;
        }
    }

    // Factory methods to create constants
    public static InfluxDBExpression createIntConstant(long val) {
        return new InfluxDBIntConstant(val);
    }

    public static InfluxDBExpression createBooleanConstant(boolean val) {
        return new InfluxDBBooleanConstant(val);
    }

    public static InfluxDBExpression createNullConstant() {
        return new InfluxDBNullConstant();
    }

    public static InfluxDBExpression createDoubleConstant(double val) {
        return new InfluxDBDoubleConstant(val);
    }

    public static InfluxDBExpression createStringConstant(String val) {
        return new InfluxDBStringConstant(val);
    }
}