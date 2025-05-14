package sqlancer.oxla.ast;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.oxla.OxlaGlobalState;
import sqlancer.oxla.schema.OxlaDataType;

import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public abstract class OxlaConstant implements OxlaExpression {
    private OxlaConstant() {
    }

    /**
     * @param toType Data type to cast the OxlaConstant to.
     * @return OxlaConstant of a given type if successful, nullptr if the cast couldn't be performed.
     */
    public abstract OxlaConstant tryCast(OxlaDataType toType);

    @Override
    public OxlaConstant getExpectedValue() {
        return this;
    }

    public static OxlaConstant getRandomForType(OxlaGlobalState state, OxlaDataType type) {
        final Randomly randomly = state.getRandomly();
        switch (type) {
            case BOOLEAN:
                return createBooleanConstant(Randomly.getBoolean());
            case DATE:
                return createDateConstant(randomly.getInteger32());
            case FLOAT32:
                return createFloat32Constant(randomly.getFloat());
            case FLOAT64:
                return createFloat64Constant(randomly.getDouble());
            case INT32:
                return createInt32Constant(randomly.getInteger32());
            case INT64:
                return createInt64Constant(randomly.getLong());
            case INTERVAL:
                return createIntervalConstant(randomly.getInteger32(), randomly.getInteger32(), randomly.getLong());
            case JSON:
                return createJsonConstant(randomly.getString());
            case TEXT:
                return createTextConstant(randomly.getString());
            case TIME:
                return createTimeConstant(randomly.getInteger32());
            case TIMESTAMP:
                return createTimestampConstant(randomly.getLong());
            case TIMESTAMPTZ:
                return createTimestamptzConstant(randomly.getLong());
            default:
                throw new AssertionError();
        }
    }

    public static OxlaConstant getRandom(OxlaGlobalState state) {
        return getRandomForType(state, OxlaDataType.getRandomType());
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
        return new OxlaFloat32Constant(value);
    }

    public static OxlaConstant createFloat64Constant(double value) {
        return new OxlaFloat64Constant(value);
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

    public static OxlaConstant createTextConstant(String text) {
        return new OxlaTextConstant(text);
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

        @Override
        public OxlaConstant tryCast(OxlaDataType unused) {
            return createNullConstant(); // No-op.
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

        @Override
        public OxlaConstant tryCast(OxlaDataType toType) {
            switch (toType) {
                case BOOLEAN:
                    return this;
                case INT32:
                    return createInt32Constant(value ? 1 : 0);
                default:
                    return null; // Impossible.
            }
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

        @Override
        public OxlaConstant tryCast(OxlaDataType toType) {
            switch (toType) {
                case DATE:
                    return this;
                case TIMESTAMP:
                    return OxlaTimestamptzConstant.createTimestampConstant(value);
                case TIMESTAMPTZ:
                    return OxlaTimestamptzConstant.createTimestamptzConstant(value);
                default:
                    return null; // Impossible.
            }
        }
    }

    public static class OxlaFloat32Constant extends OxlaConstant {
        public final float value;

        public OxlaFloat32Constant(float value) {
            this.value = value;
        }

        @Override
        public String toString() {
            if (value == Float.POSITIVE_INFINITY) {
                return "'infinity'";
            } else if (value == Float.NEGATIVE_INFINITY) {
                return "'-infinity'";
            }
            return String.valueOf(value);
        }

        @Override
        public OxlaConstant tryCast(OxlaDataType toType) {
            switch (toType) {
                case FLOAT32:
                    return this;
                case FLOAT64:
                    OxlaConstant.createFloat64Constant(value);
                case INT32:
                    OxlaConstant.createInt32Constant((int) value);
                case INT64:
                    OxlaConstant.createInt64Constant((long) value);
                case TEXT:
                    return OxlaConstant.createTextConstant(toString());
                default:
                    return null; // Impossible.
            }
        }
    }

    public static class OxlaFloat64Constant extends OxlaConstant {
        public final double value;

        public OxlaFloat64Constant(double value) {
            this.value = value;
        }

        @Override
        public String toString() {
            if (value == Double.POSITIVE_INFINITY) {
                return "'infinity'";
            } else if (value == Double.NEGATIVE_INFINITY) {
                return "'-infinity'";
            }
            return String.valueOf(value);
        }

        @Override
        public OxlaConstant tryCast(OxlaDataType toType) {
            switch (toType) {
                case FLOAT32:
                    return OxlaConstant.createFloat32Constant((float) value);
                case FLOAT64:
                    return this;
                case INT32:
                    return OxlaConstant.createInt32Constant((int) value);
                case INT64:
                    return OxlaConstant.createInt64Constant((long) value);
                case TEXT:
                    return OxlaConstant.createTextConstant(toString());
                default:
                    return null; // Impossible.
            }
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

        @Override
        public OxlaConstant tryCast(OxlaDataType toType) {
            switch (toType) {
                case BOOLEAN:
                    return OxlaConstant.createBooleanConstant(value != 0);
                case FLOAT32:
                    return OxlaConstant.createFloat32Constant(value);
                case FLOAT64:
                    return OxlaConstant.createFloat64Constant(value);
                case INT32:
                case INT64:
                    return this;
                case TEXT:
                    return OxlaConstant.createTextConstant(toString());
                default:
                    return null; // Impossible.
            }
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

        @Override
        public OxlaConstant tryCast(OxlaDataType toType) {
            switch (toType) {
                case INTERVAL:
                    return this;
                case TEXT:
                    return OxlaConstant.createTextConstant(toString());
                default:
                    return null; // Impossible.
            }
        }
    }

    public static class OxlaJsonConstant extends OxlaConstant {
        public final String value;

        public OxlaJsonConstant(String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            final String valString = value
                    .replace("'", "")
                    .replace("\\", "\\\\");
            return String.format("JSON '%s'", String.format("{\"key\":\"%s\"}", valString));
        }

        @Override
        public OxlaConstant tryCast(OxlaDataType toType) {
            switch (toType) {
                case BOOLEAN:
                    return OxlaConstant.createBooleanConstant(Boolean.parseBoolean(value));
                case FLOAT32:
                    try {
                        return OxlaConstant.createFloat32Constant(Float.parseFloat(value));
                    } catch (NumberFormatException ignored) {
                        throw new IgnoreMeException();
                    }
                case FLOAT64:
                    try {
                        return OxlaConstant.createFloat64Constant(Double.parseDouble(value));
                    } catch (NumberFormatException ignored) {
                        throw new IgnoreMeException();
                    }
                case INT32:
                    try {
                        return OxlaConstant.createInt32Constant(Integer.parseInt(value));
                    } catch (NumberFormatException ignored) {
                        throw new IgnoreMeException();
                    }
                case INT64:
                    try {
                        return OxlaConstant.createInt64Constant(Long.parseLong(value));
                    } catch (NumberFormatException ignored) {
                        throw new IgnoreMeException();
                    }
                case JSON:
                    return this;
                case TEXT:
                    return OxlaConstant.createTextConstant(toString());
                default:
                    return null; // Impossible.
            }
        }
    }

    public static class OxlaTextConstant extends OxlaConstant {
        public final String value;

        public OxlaTextConstant(String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            final String valString = value
                    .replace("'", "")
                    .replace("\\", "\\\\");
            return String.format("TEXT '%s'", valString);
        }

        @Override
        public OxlaConstant tryCast(OxlaDataType toType) {
            switch (toType) {
                case BOOLEAN:
                    try {
                        return OxlaConstant.createBooleanConstant(Boolean.parseBoolean(value));
                    } catch (NumberFormatException e) {
                        throw new IgnoreMeException();
                    }
                case DATE:
                    try {
                        return OxlaConstant.createDateConstant((int) new SimpleDateFormat("yyyy-MM-dd").parse(value).getTime());
                    } catch (ParseException e) {
                        throw new IgnoreMeException();
                    }
                case FLOAT32:
                    try {
                        return OxlaConstant.createFloat32Constant(Float.parseFloat(value));
                    } catch (NumberFormatException e) {
                        throw new IgnoreMeException();
                    }
                case FLOAT64:
                    try {
                        return OxlaConstant.createFloat64Constant(Double.parseDouble(value));
                    } catch (NumberFormatException e) {
                        throw new IgnoreMeException();
                    }
                case INT32:
                    try {
                        return OxlaConstant.createInt32Constant(Integer.parseInt(value));
                    } catch (NumberFormatException e) {
                        throw new IgnoreMeException();
                    }
                case INT64:
                    try {
                        return OxlaConstant.createInt64Constant(Long.parseLong(value));
                    } catch (NumberFormatException e) {
                        throw new IgnoreMeException();
                    }
                case INTERVAL:
                    // FIXME: Invalid for now; silently ignore the query that tries this.
                    //        Implementing this correctly would require me to duplicate our `parseInterval` code here,
                    //        which would take a lot of time, also we cannot do it naively here in our defined toString()
                    //        format, because the potential values can come from the database itself.
                    throw new IgnoreMeException();
                case JSON:
                    return OxlaConstant.createJsonConstant(value);
                case TEXT:
                    return this;
                case TIME:
                    try {
                        return OxlaConstant.createTimeConstant((int) new SimpleDateFormat("HH:mm::ss").parse(value).getTime());
                    } catch (ParseException e) {
                        throw new IgnoreMeException();
                    }
                case TIMESTAMP:
                    try {
                        return OxlaConstant.createTimestampConstant(new SimpleDateFormat("yyyy-MM-dd HH:mm::ss").parse(value).getTime());
                    } catch (ParseException e) {
                        throw new IgnoreMeException();
                    }
                case TIMESTAMPTZ:
                    try {
                        return OxlaConstant.createTimestamptzConstant(new SimpleDateFormat("yyyy-MM-dd HH:mm::ss+00").parse(value).getTime());
                    } catch (ParseException e) {
                        throw new IgnoreMeException();
                    }
                default:
                    return null; // Impossible.
            }
        }
    }

    public static class OxlaTimeConstant extends OxlaConstant {
        public final int value;

        public OxlaTimeConstant(int value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return String.format("TIME '%s'", new Time(value));
        }

        @Override
        public OxlaConstant tryCast(OxlaDataType toType) {
            switch (toType) {
                case INTERVAL:
                    return OxlaConstant.createIntervalConstant(0, 0, value);
                case TEXT:
                    return OxlaConstant.createTextConstant(toString());
                case TIME:
                    return this;
                default:
                    return null; // Impossible.
            }
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

        @Override
        public OxlaConstant tryCast(OxlaDataType toType) {
            switch (toType) {
                case DATE:
                    return OxlaConstant.createDateConstant((int) value);
                case TEXT:
                    return OxlaConstant.createTextConstant(toString());
                case TIME:
                    return OxlaConstant.createTimeConstant((int) value);
                case TIMESTAMP:
                    return this;
                case TIMESTAMPTZ:
                    return OxlaConstant.createTimestamptzConstant(value);
                default:
                    return null; // Impossible.
            }
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

        @Override
        public OxlaConstant tryCast(OxlaDataType toType) {
            switch (toType) {
                case DATE:
                    return OxlaConstant.createDateConstant((int) value);
                case TEXT:
                    return OxlaConstant.createTextConstant(toString());
                case TIME:
                    return OxlaConstant.createTimeConstant((int) value);
                case TIMESTAMP:
                    return OxlaConstant.createTimestampConstant(value);
                case TIMESTAMPTZ:
                    return this;
                default:
                    return null; // Impossible.
            }
        }
    }
}
