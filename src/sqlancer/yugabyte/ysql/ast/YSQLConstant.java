package sqlancer.yugabyte.ysql.ast;

import java.math.BigDecimal;

import sqlancer.IgnoreMeException;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLDataType;

public abstract class YSQLConstant implements YSQLExpression {

    public static YSQLConstant createNullConstant() {
        return new YSQLNullConstant();
    }

    public static YSQLConstant createIntConstant(long val) {
        return new IntConstant(val);
    }

    public static YSQLConstant createBooleanConstant(boolean val) {
        return new BooleanConstant(val);
    }

    public static YSQLConstant createFalse() {
        return createBooleanConstant(false);
    }

    public static YSQLConstant createTrue() {
        return createBooleanConstant(true);
    }

    public static YSQLConstant createTextConstant(String string) {
        return new StringConstant(string);
    }

    public static YSQLConstant createByteConstant(String string) {
        return new ByteConstant(string);
    }

    public static YSQLConstant createDecimalConstant(BigDecimal bigDecimal) {
        return new DecimalConstant(bigDecimal);
    }

    public static YSQLConstant createFloatConstant(float val) {
        return new FloatConstant(val);
    }

    public static YSQLConstant createDoubleConstant(double val) {
        return new DoubleConstant(val);
    }

    public static YSQLConstant createRange(long left, boolean leftIsInclusive, long right, boolean rightIsInclusive) {
        long realLeft;
        long realRight;
        if (left > right) {
            realRight = left;
            realLeft = right;
        } else {
            realLeft = left;
            realRight = right;
        }
        return new RangeConstant(realLeft, leftIsInclusive, realRight, rightIsInclusive);
    }

    public static YSQLExpression createBitConstant(long integer) {
        return new BitConstant(integer);
    }

    public static YSQLExpression createInetConstant(String val) {
        return new InetConstant(val);
    }

    public abstract String getTextRepresentation();

    public abstract String getUnquotedTextRepresentation();

    public String asString() {
        throw new UnsupportedOperationException(this.toString());
    }

    public boolean isString() {
        return false;
    }

    @Override
    public YSQLConstant getExpectedValue() {
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

    public boolean isBoolean() {
        return false;
    }

    public abstract YSQLConstant isEquals(YSQLConstant rightVal);

    public boolean isInt() {
        return false;
    }

    protected abstract YSQLConstant isLessThan(YSQLConstant rightVal);

    @Override
    public String toString() {
        return getTextRepresentation();
    }

    public abstract YSQLConstant cast(YSQLDataType type);

    public static class BooleanConstant extends YSQLConstant {

        private final boolean value;

        public BooleanConstant(boolean value) {
            this.value = value;
        }

        @Override
        public String getTextRepresentation() {
            return value ? "TRUE" : "FALSE";
        }

        @Override
        public String getUnquotedTextRepresentation() {
            return getTextRepresentation();
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
        public YSQLConstant isEquals(YSQLConstant rightVal) {
            if (rightVal.isNull()) {
                return YSQLConstant.createNullConstant();
            } else if (rightVal.isBoolean()) {
                return YSQLConstant.createBooleanConstant(value == rightVal.asBoolean());
            } else if (rightVal.isString()) {
                return YSQLConstant.createBooleanConstant(value == rightVal.cast(YSQLDataType.BOOLEAN).asBoolean());
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        protected YSQLConstant isLessThan(YSQLConstant rightVal) {
            if (rightVal.isNull()) {
                return YSQLConstant.createNullConstant();
            } else if (rightVal.isString()) {
                return isLessThan(rightVal.cast(YSQLDataType.BOOLEAN));
            } else {
                assert rightVal.isBoolean();
                return YSQLConstant.createBooleanConstant((value ? 1 : 0) < (rightVal.asBoolean() ? 1 : 0));
            }
        }

        @Override
        public YSQLConstant cast(YSQLDataType type) {
            switch (type) {
            case BOOLEAN:
                return this;
            case INT:
                return YSQLConstant.createIntConstant(value ? 1 : 0);
            case TEXT:
                return YSQLConstant.createTextConstant(value ? "true" : "false");
            default:
                return null;
            }
        }

        @Override
        public YSQLDataType getExpressionType() {
            return YSQLDataType.BOOLEAN;
        }

    }

    public static class YSQLNullConstant extends YSQLConstant {

        @Override
        public String getTextRepresentation() {
            return "NULL";
        }

        @Override
        public String getUnquotedTextRepresentation() {
            return getTextRepresentation();
        }

        @Override
        public boolean isNull() {
            return true;
        }

        @Override
        public YSQLConstant isEquals(YSQLConstant rightVal) {
            return YSQLConstant.createNullConstant();
        }

        @Override
        protected YSQLConstant isLessThan(YSQLConstant rightVal) {
            return YSQLConstant.createNullConstant();
        }

        @Override
        public YSQLConstant cast(YSQLDataType type) {
            return YSQLConstant.createNullConstant();
        }

        @Override
        public YSQLDataType getExpressionType() {
            return null;
        }

    }

    public static class StringConstant extends YSQLConstant {

        protected final String value;

        public StringConstant(String value) {
            this.value = value;
        }

        @Override
        public String getTextRepresentation() {
            return String.format("'%s'", value.replace("'", "''"));
        }

        @Override
        public String getUnquotedTextRepresentation() {
            return value;
        }

        @Override
        public String asString() {
            return value;
        }

        @Override
        public boolean isString() {
            return true;
        }

        @Override
        public YSQLConstant isEquals(YSQLConstant rightVal) {
            if (rightVal.isNull()) {
                return YSQLConstant.createNullConstant();
            } else if (rightVal.isInt()) {
                return cast(YSQLDataType.INT).isEquals(rightVal.cast(YSQLDataType.INT));
            } else if (rightVal.isBoolean()) {
                return cast(YSQLDataType.BOOLEAN).isEquals(rightVal.cast(YSQLDataType.BOOLEAN));
            } else if (rightVal.isString()) {
                return YSQLConstant.createBooleanConstant(value.contentEquals(rightVal.asString()));
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        protected YSQLConstant isLessThan(YSQLConstant rightVal) {
            if (rightVal.isNull()) {
                return YSQLConstant.createNullConstant();
            } else if (rightVal.isInt()) {
                return cast(YSQLDataType.INT).isLessThan(rightVal.cast(YSQLDataType.INT));
            } else if (rightVal.isBoolean()) {
                return cast(YSQLDataType.BOOLEAN).isLessThan(rightVal.cast(YSQLDataType.BOOLEAN));
            } else if (rightVal.isString()) {
                return YSQLConstant.createBooleanConstant(value.compareTo(rightVal.asString()) < 0);
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        public YSQLConstant cast(YSQLDataType type) {
            if (type == YSQLDataType.TEXT) {
                return this;
            }
            String s = value.trim();
            switch (type) {
            case BOOLEAN:
                try {
                    return YSQLConstant.createBooleanConstant(Long.parseLong(s) != 0);
                } catch (NumberFormatException e) {
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
                    return YSQLConstant.createTrue();
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
                    return YSQLConstant.createFalse();
                }
            case INT:
                try {
                    return YSQLConstant.createIntConstant(Long.parseLong(s));
                } catch (NumberFormatException e) {
                    return YSQLConstant.createIntConstant(-1);
                }
            case TEXT:
                return this;
            default:
                return null;
            }
        }

        @Override
        public YSQLDataType getExpressionType() {
            return YSQLDataType.TEXT;
        }

    }

    public static class IntConstant extends YSQLConstant {

        private final long val;

        public IntConstant(long val) {
            this.val = val;
        }

        @Override
        public String getTextRepresentation() {
            return String.valueOf(val);
        }

        @Override
        public String getUnquotedTextRepresentation() {
            return getTextRepresentation();
        }

        @Override
        public long asInt() {
            return val;
        }

        @Override
        public YSQLConstant isEquals(YSQLConstant rightVal) {
            if (rightVal.isNull()) {
                return YSQLConstant.createNullConstant();
            } else if (rightVal.isBoolean()) {
                return cast(YSQLDataType.BOOLEAN).isEquals(rightVal);
            } else if (rightVal.isInt()) {
                return YSQLConstant.createBooleanConstant(val == rightVal.asInt());
            } else if (rightVal.isString()) {
                return YSQLConstant.createBooleanConstant(val == rightVal.cast(YSQLDataType.INT).asInt());
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        public boolean isInt() {
            return true;
        }

        @Override
        protected YSQLConstant isLessThan(YSQLConstant rightVal) {
            if (rightVal.isNull()) {
                return YSQLConstant.createNullConstant();
            } else if (rightVal.isInt()) {
                return YSQLConstant.createBooleanConstant(val < rightVal.asInt());
            } else if (rightVal.isBoolean()) {
                throw new AssertionError(rightVal);
            } else if (rightVal.isString()) {
                return YSQLConstant.createBooleanConstant(val < rightVal.cast(YSQLDataType.INT).asInt());
            } else {
                throw new IgnoreMeException();
            }

        }

        @Override
        public YSQLConstant cast(YSQLDataType type) {
            switch (type) {
            case BOOLEAN:
                return YSQLConstant.createBooleanConstant(val != 0);
            case INT:
                return this;
            case TEXT:
                return YSQLConstant.createTextConstant(String.valueOf(val));
            default:
                return null;
            }
        }

        @Override
        public YSQLDataType getExpressionType() {
            return YSQLDataType.INT;
        }

    }

    public static class ByteConstant extends StringConstant {

        public ByteConstant(String value) {
            super(value);
        }

        @Override
        public String getTextRepresentation() {
            return String.format("'%s'::bytea", value.replace("'", "''"));
        }
    }

    public abstract static class YSQLConstantBase extends YSQLConstant {

        @Override
        public String getUnquotedTextRepresentation() {
            return null;
        }

        @Override
        public YSQLConstant isEquals(YSQLConstant rightVal) {
            return null;
        }

        @Override
        protected YSQLConstant isLessThan(YSQLConstant rightVal) {
            return null;
        }

        @Override
        public YSQLConstant cast(YSQLDataType type) {
            return null;
        }
    }

    public static class DecimalConstant extends YSQLConstantBase {

        private final BigDecimal val;

        public DecimalConstant(BigDecimal val) {
            this.val = val;
        }

        @Override
        public String getTextRepresentation() {
            return String.valueOf(val);
        }

        @Override
        public YSQLDataType getExpressionType() {
            return YSQLDataType.DECIMAL;
        }

    }

    public static class InetConstant extends YSQLConstantBase {

        private final String val;

        public InetConstant(String val) {
            this.val = "'" + val + "'";
        }

        @Override
        public String getTextRepresentation() {
            return val;
        }

        @Override
        public YSQLDataType getExpressionType() {
            return YSQLDataType.INET;
        }

    }

    public static class FloatConstant extends YSQLConstantBase {

        private final float val;

        public FloatConstant(float val) {
            this.val = val;
        }

        @Override
        public String getTextRepresentation() {
            if (Double.isFinite(val)) {
                return String.valueOf(val);
            } else {
                return "'" + val + "'";
            }
        }

        @Override
        public YSQLDataType getExpressionType() {
            return YSQLDataType.FLOAT;
        }

    }

    public static class DoubleConstant extends YSQLConstantBase {

        private final double val;

        public DoubleConstant(double val) {
            this.val = val;
        }

        @Override
        public String getTextRepresentation() {
            if (Double.isFinite(val)) {
                return String.valueOf(val);
            } else {
                return "'" + val + "'";
            }
        }

        @Override
        public YSQLDataType getExpressionType() {
            return YSQLDataType.FLOAT;
        }

    }

    public static class BitConstant extends YSQLConstantBase {

        private final long val;

        public BitConstant(long val) {
            this.val = val;
        }

        @Override
        public String getTextRepresentation() {
            return String.format("B'%s'", Long.toBinaryString(val));
        }

        @Override
        public YSQLDataType getExpressionType() {
            return YSQLDataType.BIT;
        }

    }

    public static class RangeConstant extends YSQLConstantBase {

        private final long left;
        private final boolean leftIsInclusive;
        private final long right;
        private final boolean rightIsInclusive;

        public RangeConstant(long left, boolean leftIsInclusive, long right, boolean rightIsInclusive) {
            this.left = left;
            this.leftIsInclusive = leftIsInclusive;
            this.right = right;
            this.rightIsInclusive = rightIsInclusive;
        }

        @Override
        public String getTextRepresentation() {
            StringBuilder sb = new StringBuilder();
            sb.append("'");
            if (leftIsInclusive) {
                sb.append("[");
            } else {
                sb.append("(");
            }
            sb.append(left);
            sb.append(",");
            sb.append(right);
            if (rightIsInclusive) {
                sb.append("]");
            } else {
                sb.append(")");
            }
            sb.append("'");
            sb.append("::int4range");
            return sb.toString();
        }

        @Override
        public YSQLDataType getExpressionType() {
            return YSQLDataType.RANGE;
        }

    }

}
