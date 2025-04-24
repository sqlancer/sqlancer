package sqlancer.oxla.ast;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;

public abstract class OxlaConstant implements OxlaExpression {

    private OxlaConstant() {
    }

    public static OxlaConstant createNullConstant() {
        return new OxlaNullConstant();
    }

    public static OxlaConstant createBooleanConstant(boolean value) {
        return new OxlaBooleanConstant(value);
    }

    public static OxlaConstant createDateConstant(int value) {
        return new OxlaDateConstant(value);
    }

    public static OxlaConstant createFloat32Constant(float value) {
        return new OxlaFloatConstant(value);
    }

    public static OxlaConstant createFloat64Constant(double value) {
        return new OxlaFloatConstant(value);
    }

    public static OxlaConstant createInt32Constant(int value) {
        return new OxlaIntegerConstant(value);
    }

    public static OxlaConstant createInt64Constant(long value) {
        return new OxlaIntegerConstant(value);
    }

    public static OxlaConstant createIntervalConstant(int months, int days, long microseconds) {
        return new OxlaIntervalConstant(months, days, microseconds);
    }

    public static OxlaConstant createJsonConstant(String json) {
        return new OxlaJsonConstant(json);
    }

    public static OxlaConstant createTextConstant(String json) {
        return new OxlaTextConstant(json);
    }

    public static OxlaConstant createTimeConstant(int value) {
        return new OxlaTimeConstant(value);
    }

    public static OxlaConstant createTimestampConstant(long value) {
        return new OxlaTimestampConstant(value);
    }

    public static OxlaConstant createTimestamptzConstant(long value) {
        return new OxlaTimestamptzConstant(value);
    }

    //
    //
    //

    public static class OxlaNullConstant extends OxlaConstant {
        @Override
        public String toString() {
            return "NULL";
        }
    }

    public static class OxlaBooleanConstant extends OxlaConstant {
        public final boolean value;

        public OxlaBooleanConstant(boolean value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }
    }

    public static class OxlaDateConstant extends OxlaConstant {
        public final int value;

        public OxlaDateConstant(int value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return String.format("DATE '%s'", new SimpleDateFormat("yyyy-MM-dd").format(new Timestamp(value)));
        }
    }

    public static class OxlaFloatConstant extends OxlaConstant {
        public final double value;

        public OxlaFloatConstant(double value) {
            this.value = value;
        }

        public OxlaFloatConstant(float value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }
    }

    public static class OxlaIntegerConstant extends OxlaConstant {
        public final long value;

        public OxlaIntegerConstant(long value) {
            this.value = value;
        }

        public OxlaIntegerConstant(int value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }
    }

    public static class OxlaIntervalConstant extends OxlaConstant {
        public final int months;
        public final int days;
        public final long microseconds;

        public OxlaIntervalConstant(int months, int days, long microseconds) {
            this.months = months;
            this.days = days;
            this.microseconds = microseconds;
        }

        @Override
        public String toString() {
            return String.format("INTERVAL '%d months %d days %d microseconds'", months, days, microseconds);
        }
    }

    public static class OxlaJsonConstant extends OxlaConstant {
        public final String value;

        public OxlaJsonConstant(String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return String.format("JSON '%s'", value);
        }
    }

    public static class OxlaTextConstant extends OxlaConstant {
        public final String value;

        public OxlaTextConstant(String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return value;
        }
    }

    public static class OxlaTimeConstant extends OxlaConstant {
        public final int value;

        public OxlaTimeConstant(int value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return String.format("TIME '%s'", new SimpleDateFormat("yyyy-MM-dd").format(new Timestamp(value)));
        }
    }

    public static class OxlaTimestampConstant extends OxlaConstant {
        public final long value;

        public OxlaTimestampConstant(long value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return String.format("TIMESTAMP WITHOUT TIME ZONE '%s'", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(value));
        }
    }

    public static class OxlaTimestamptzConstant extends OxlaConstant {
        public final long value;

        public OxlaTimestamptzConstant(long value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return String.format("TIMESTAMP WITH TIME ZONE '%s'", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss+00").format(value));
        }
    }
}
