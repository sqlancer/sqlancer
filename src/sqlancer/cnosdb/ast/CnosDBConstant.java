package sqlancer.cnosdb.ast;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Date;

import sqlancer.IgnoreMeException;
import sqlancer.cnosdb.CnosDBSchema.CnosDBDataType;

public abstract class CnosDBConstant implements CnosDBExpression {

    public static CnosDBConstant createNullConstant() {
        return new CnosDBNullConstant();
    }

    public static CnosDBConstant createIntConstant(long val) {
        return new IntConstant(val, false);
    }

    public static CnosDBConstant createBooleanConstant(boolean val) {
        return new BooleanConstant(val);
    }

    public static CnosDBConstant createFalse() {
        return createBooleanConstant(false);
    }

    public static CnosDBConstant createTrue() {
        return createBooleanConstant(true);
    }

    public static CnosDBConstant createStringConstant(String string) {
        return new StringConstant(string);
    }

    public static CnosDBConstant createDoubleConstant(double val) {
        return new DoubleConstant(val);
    }

    public static CnosDBConstant createUintConstant(long val) {
        return new IntConstant(val, true);
    }

    public static CnosDBConstant createTimeStampConstant(long val) {
        return new TimeStampConstant(val);
    }

    public abstract String getTextRepresentation();

    public String getUnquotedTextRepresentation() {
        return getTextRepresentation();
    }

    public String asString() {
        throw new UnsupportedOperationException(this.toString());
    }

    public boolean isString() {
        return false;
    }

    @Override
    public CnosDBConstant getExpectedValue() {
        return this;
    }

    public boolean isNull() {
        return false;
    }

    public boolean asBoolean() {
        throw new UnsupportedOperationException(this.toString());
    }

    public long asInt() {
        throw new UnsupportedOperationException(this.toString());
    }

    public double asDouble() {
        throw new UnsupportedOperationException(this.toString());
    }

    public boolean isBoolean() {
        return false;
    }

    public abstract CnosDBConstant isEquals(CnosDBConstant rightVal);

    public boolean isInt() {
        return false;
    }

    protected abstract CnosDBConstant isLessThan(CnosDBConstant rightVal);

    @Override
    public String toString() {
        return getTextRepresentation();
    }

    public abstract CnosDBConstant cast(CnosDBDataType type);

    public static class BooleanConstant extends CnosDBConstant {

        private final boolean value;

        public BooleanConstant(boolean value) {
            this.value = value;
        }

        @Override
        public String getTextRepresentation() {
            return value ? "TRUE" : "FALSE";
        }

        @Override
        public CnosDBDataType getExpressionType() {
            return CnosDBDataType.BOOLEAN;
        }

        @Override
        public boolean asBoolean() {
            return value;
        }

        @Override
        public boolean isBoolean() {
            return true;
        }

        @Override
        public CnosDBConstant isEquals(CnosDBConstant rightVal) {
            if (rightVal.isNull()) {
                return CnosDBConstant.createNullConstant();
            } else if (rightVal.isBoolean()) {
                return CnosDBConstant.createBooleanConstant(value == rightVal.asBoolean());
            } else if (rightVal.isString()) {
                return CnosDBConstant.createBooleanConstant(value == rightVal.cast(CnosDBDataType.BOOLEAN).asBoolean());
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        protected CnosDBConstant isLessThan(CnosDBConstant rightVal) {
            if (rightVal.isNull()) {
                return CnosDBConstant.createNullConstant();
            } else if (rightVal.isString()) {
                return isLessThan(rightVal.cast(CnosDBDataType.BOOLEAN));
            } else {
                assert rightVal.isBoolean();
                return CnosDBConstant.createBooleanConstant((value ? 1 : 0) < (rightVal.asBoolean() ? 1 : 0));
            }
        }

        @Override
        public CnosDBConstant cast(CnosDBDataType type) {
            switch (type) {
            case BOOLEAN:
                return this;
            case INT:
                return CnosDBConstant.createIntConstant(value ? 1 : 0);
            case UINT:
                return CnosDBConstant.createUintConstant(value ? 1 : 0);
            case STRING:
                return CnosDBConstant.createStringConstant(value ? "true" : "false");
            default:
                return null;
            }
        }

    }

    public static class CnosDBNullConstant extends CnosDBConstant {

        @Override
        public String getTextRepresentation() {
            return "NULL";
        }

        @Override
        public CnosDBDataType getExpressionType() {
            return null;
        }

        @Override
        public boolean isNull() {
            return true;
        }

        @Override
        public CnosDBConstant isEquals(CnosDBConstant rightVal) {
            return CnosDBConstant.createNullConstant();
        }

        @Override
        protected CnosDBConstant isLessThan(CnosDBConstant rightVal) {
            return CnosDBConstant.createNullConstant();
        }

        @Override
        public CnosDBConstant cast(CnosDBDataType type) {
            return CnosDBConstant.createNullConstant();
        }
    }

    public static class StringConstant extends CnosDBConstant {

        private final String value;

        public StringConstant(String value) {
            this.value = value;
        }

        @Override
        public String getTextRepresentation() {
            return String.format("'%s'", value.replace("'", "''"));
        }

        @Override
        public CnosDBConstant isEquals(CnosDBConstant rightVal) {
            if (rightVal.isNull()) {
                return CnosDBConstant.createNullConstant();
            } else if (rightVal.isInt()) {
                return cast(CnosDBDataType.INT).isEquals(rightVal.cast(CnosDBDataType.INT));
            } else if (rightVal.isBoolean()) {
                return cast(CnosDBDataType.BOOLEAN).isEquals(rightVal.cast(CnosDBDataType.BOOLEAN));
            } else if (rightVal.isString()) {
                return CnosDBConstant.createBooleanConstant(value.contentEquals(rightVal.asString()));
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        protected CnosDBConstant isLessThan(CnosDBConstant rightVal) {
            if (rightVal.isNull()) {
                return CnosDBConstant.createNullConstant();
            } else if (rightVal.isInt()) {
                return cast(CnosDBDataType.INT).isLessThan(rightVal.cast(CnosDBDataType.INT));
            } else if (rightVal.isBoolean()) {
                return cast(CnosDBDataType.BOOLEAN).isLessThan(rightVal.cast(CnosDBDataType.BOOLEAN));
            } else if (rightVal.isString()) {
                return CnosDBConstant.createBooleanConstant(value.compareTo(rightVal.asString()) < 0);
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        public CnosDBConstant cast(CnosDBDataType type) {
            if (type == CnosDBDataType.STRING) {
                return this;
            }
            String s = value.trim();
            switch (type) {
            case BOOLEAN:
                try {
                    return CnosDBConstant.createBooleanConstant(Long.parseLong(s) != 0);
                } catch (NumberFormatException ignored) {
                }
                switch (s.toUpperCase()) {
                case "T":
                case "TR":
                case "TRU":
                case "TRUE":
                case "1":
                case "YES":
                case "YE":
                case "Y":
                case "ON":
                    return CnosDBConstant.createTrue();
                case "F":
                case "FA":
                case "FAL":
                case "FALS":
                case "FALSE":
                case "N":
                case "NO":
                case "OF":
                case "OFF":
                default:
                    return CnosDBConstant.createFalse();
                }
            case INT:
                try {
                    return CnosDBConstant.createIntConstant(Long.parseLong(s));
                } catch (NumberFormatException e) {
                    return CnosDBConstant.createIntConstant(-1);
                }
            case UINT:
                try {
                    return CnosDBConstant.createUintConstant(Long.parseUnsignedLong(s));
                } catch (NumberFormatException e) {
                    return CnosDBConstant.createUintConstant(0);
                }
            case DOUBLE:
                try {
                    return CnosDBConstant.createDoubleConstant(Double.parseDouble(s));
                } catch (NumberFormatException e) {
                    return CnosDBConstant.createDoubleConstant(0.0);
                }

            default:
                return null;
            }
        }

        @Override
        public CnosDBDataType getExpressionType() {
            return CnosDBDataType.STRING;
        }

        @Override
        public boolean isString() {
            return true;
        }

        @Override
        public String asString() {
            return value;
        }

        @Override
        public String getUnquotedTextRepresentation() {
            return value;
        }

    }

    public static class IntConstant extends CnosDBConstant {

        private final long val;
        private final boolean unsigned;

        public IntConstant(long val, boolean unsigned) {
            this.val = val;
            this.unsigned = unsigned;
        }

        @Override
        public String getTextRepresentation() {
            if (unsigned) {
                return Long.toUnsignedString(val);
            } else {
                return String.valueOf(val);
            }
        }

        @Override
        public CnosDBDataType getExpressionType() {
            if (unsigned) {
                return CnosDBDataType.UINT;
            }
            return CnosDBDataType.INT;
        }

        @Override
        public long asInt() {
            return val;
        }

        @Override
        public double asDouble() {
            return val;
        }

        @Override
        public boolean isInt() {
            return true;
        }

        @Override
        public CnosDBConstant isEquals(CnosDBConstant rightVal) {
            if (rightVal.isNull()) {
                return CnosDBConstant.createNullConstant();
            } else if (rightVal.isBoolean()) {
                return cast(CnosDBDataType.BOOLEAN).isEquals(rightVal);
            } else if (rightVal.isInt()) {
                return CnosDBConstant.createBooleanConstant(val == rightVal.asInt());
            } else if (rightVal.isString()) {
                return CnosDBConstant.createBooleanConstant(val == rightVal.cast(CnosDBDataType.INT).asInt());
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        protected CnosDBConstant isLessThan(CnosDBConstant rightVal) {
            if (rightVal.isNull()) {
                return CnosDBConstant.createNullConstant();
            } else if (rightVal.isInt()) {
                return CnosDBConstant.createBooleanConstant(val < rightVal.asInt());
            } else if (rightVal.isBoolean()) {
                throw new AssertionError(rightVal);
            } else if (rightVal.getExpressionType() == CnosDBDataType.UINT) {
                return CnosDBConstant.createBooleanConstant(Long.compareUnsigned(val, rightVal.asInt()) < 0);
            } else if (rightVal.isString()) {
                return CnosDBConstant.createBooleanConstant(val < rightVal.cast(CnosDBDataType.INT).asInt());
            } else {
                throw new IgnoreMeException();
            }

        }

        @Override
        public CnosDBConstant cast(CnosDBDataType type) {
            switch (type) {
            case BOOLEAN:
                return CnosDBConstant.createBooleanConstant(val != 0);
            case INT:
                return CnosDBConstant.createIntConstant(val);
            case STRING:
                return CnosDBConstant.createStringConstant(String.valueOf(val));
            case UINT:
                return CnosDBConstant.createUintConstant(val);
            case DOUBLE:
                return CnosDBConstant.createDoubleConstant(val);
            default:
                return null;
            }
        }
    }

    public static class TimeStampConstant extends CnosDBConstant {
        final long val;

        TimeStampConstant(long time) {
            val = time;
        }

        @Override
        public String getTextRepresentation() {
            return getUnquotedTextRepresentation();
        }

        @Override
        public String getUnquotedTextRepresentation() {
            return "CAST (" + val + " AS TIMESTAMP)";
        }

        @Override
        public CnosDBConstant isEquals(CnosDBConstant rightVal) {
            if (rightVal.isNull()) {
                return createNullConstant();
            } else if (rightVal.getExpressionType() == CnosDBDataType.TIMESTAMP) {
                return createBooleanConstant(val == rightVal.asInt());
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        protected CnosDBConstant isLessThan(CnosDBConstant rightVal) {
            if (rightVal.isNull()) {
                return CnosDBConstant.createNullConstant();
            } else if (rightVal.getExpressionType() == CnosDBDataType.TIMESTAMP) {
                return CnosDBConstant.createBooleanConstant(val < rightVal.asInt());
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        public CnosDBConstant cast(CnosDBDataType type) {
            switch (type) {
            case INT:
                return createIntConstant(val);
            case STRING:
                final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
                return CnosDBConstant.createStringConstant(dateFormat.format(new Date(val)));
            default:
                return null;
            }
        }

        @Override
        public long asInt() {
            return val;
        }
    }

    public static class DoubleConstant extends CnosDBConstant {

        private final double val;

        public DoubleConstant(double val) {
            this.val = val;
        }

        @Override
        public String getTextRepresentation() {
            if (Double.isFinite(val)) {
                BigDecimal bigDecimal = new BigDecimal(val);
                return bigDecimal.toPlainString();
            } else {
                return String.valueOf(0.0);
            }
        }

        @Override
        public CnosDBDataType getExpressionType() {
            return CnosDBDataType.DOUBLE;
        }

        @Override
        public boolean isNull() {
            return false;
        }

        @Override
        protected CnosDBConstant isLessThan(CnosDBConstant rightVal) {
            if (rightVal.isNull()) {
                return CnosDBConstant.createNullConstant();
            } else if (rightVal.isBoolean()) {
                return cast(CnosDBDataType.BOOLEAN).isLessThan(rightVal);
            } else {
                return CnosDBConstant.createBooleanConstant(val < rightVal.cast(CnosDBDataType.DOUBLE).asDouble());
            }
        }

        @Override
        public CnosDBConstant isEquals(CnosDBConstant rightVal) {
            if (rightVal.isNull()) {
                return CnosDBConstant.createNullConstant();
            } else if (rightVal.isBoolean()) {
                return cast(CnosDBDataType.BOOLEAN).isEquals(rightVal);
            } else {
                return CnosDBConstant.createBooleanConstant(val == rightVal.cast(CnosDBDataType.DOUBLE).asDouble());
            }
        }

        @Override
        public CnosDBConstant cast(CnosDBDataType type) {
            return null;
        }
    }

}
