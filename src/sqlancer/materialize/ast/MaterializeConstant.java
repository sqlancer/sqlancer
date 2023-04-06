package sqlancer.materialize.ast;

import java.math.BigDecimal;

import sqlancer.IgnoreMeException;
import sqlancer.materialize.MaterializeSchema.MaterializeDataType;

public abstract class MaterializeConstant implements MaterializeExpression {

    public abstract String getTextRepresentation();

    public abstract String getUnquotedTextRepresentation();

    public static class BooleanConstant extends MaterializeConstant {

        private final boolean value;

        public BooleanConstant(boolean value) {
            this.value = value;
        }

        @Override
        public String getTextRepresentation() {
            return value ? "TRUE" : "FALSE";
        }

        @Override
        public MaterializeDataType getExpressionType() {
            return MaterializeDataType.BOOLEAN;
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
        public MaterializeConstant isEquals(MaterializeConstant rightVal) {
            if (rightVal.isNull()) {
                return MaterializeConstant.createNullConstant();
            } else if (rightVal.isBoolean()) {
                return MaterializeConstant.createBooleanConstant(value == rightVal.asBoolean());
            } else if (rightVal.isString()) {
                return MaterializeConstant
                        .createBooleanConstant(value == rightVal.cast(MaterializeDataType.BOOLEAN).asBoolean());
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        protected MaterializeConstant isLessThan(MaterializeConstant rightVal) {
            if (rightVal.isNull()) {
                return MaterializeConstant.createNullConstant();
            } else if (rightVal.isString()) {
                return isLessThan(rightVal.cast(MaterializeDataType.BOOLEAN));
            } else {
                assert rightVal.isBoolean();
                return MaterializeConstant.createBooleanConstant((value ? 1 : 0) < (rightVal.asBoolean() ? 1 : 0));
            }
        }

        @Override
        public MaterializeConstant cast(MaterializeDataType type) {
            switch (type) {
            case BOOLEAN:
                return this;
            case INT:
                return MaterializeConstant.createIntConstant(value ? 1 : 0);
            case TEXT:
                return MaterializeConstant.createTextConstant(value ? "true" : "false");
            default:
                return null;
            }
        }

        @Override
        public String getUnquotedTextRepresentation() {
            return getTextRepresentation();
        }

    }

    public static class MaterializeNullConstant extends MaterializeConstant {

        @Override
        public String getTextRepresentation() {
            return "NULL";
        }

        @Override
        public MaterializeDataType getExpressionType() {
            return null;
        }

        @Override
        public boolean isNull() {
            return true;
        }

        @Override
        public MaterializeConstant isEquals(MaterializeConstant rightVal) {
            return MaterializeConstant.createNullConstant();
        }

        @Override
        protected MaterializeConstant isLessThan(MaterializeConstant rightVal) {
            return MaterializeConstant.createNullConstant();
        }

        @Override
        public MaterializeConstant cast(MaterializeDataType type) {
            return MaterializeConstant.createNullConstant();
        }

        @Override
        public String getUnquotedTextRepresentation() {
            return getTextRepresentation();
        }

    }

    public static class StringConstant extends MaterializeConstant {

        private final String value;

        public StringConstant(String value) {
            this.value = value;
        }

        @Override
        public String getTextRepresentation() {
            return String.format("'%s'", value.replace("'", "''"));
        }

        @Override
        public MaterializeConstant isEquals(MaterializeConstant rightVal) {
            if (rightVal.isNull()) {
                return MaterializeConstant.createNullConstant();
            } else if (rightVal.isInt()) {
                return cast(MaterializeDataType.INT).isEquals(rightVal.cast(MaterializeDataType.INT));
            } else if (rightVal.isBoolean()) {
                return cast(MaterializeDataType.BOOLEAN).isEquals(rightVal.cast(MaterializeDataType.BOOLEAN));
            } else if (rightVal.isString()) {
                return MaterializeConstant.createBooleanConstant(value.contentEquals(rightVal.asString()));
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        protected MaterializeConstant isLessThan(MaterializeConstant rightVal) {
            if (rightVal.isNull()) {
                return MaterializeConstant.createNullConstant();
            } else if (rightVal.isInt()) {
                return cast(MaterializeDataType.INT).isLessThan(rightVal.cast(MaterializeDataType.INT));
            } else if (rightVal.isBoolean()) {
                return cast(MaterializeDataType.BOOLEAN).isLessThan(rightVal.cast(MaterializeDataType.BOOLEAN));
            } else if (rightVal.isString()) {
                return MaterializeConstant.createBooleanConstant(value.compareTo(rightVal.asString()) < 0);
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        public MaterializeConstant cast(MaterializeDataType type) {
            if (type == MaterializeDataType.TEXT) {
                return this;
            }
            String s = value.trim();
            switch (type) {
            case BOOLEAN:
                try {
                    return MaterializeConstant.createBooleanConstant(Long.parseLong(s) != 0);
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
                    return MaterializeConstant.createTrue();
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
                    return MaterializeConstant.createFalse();
                }
            case INT:
                try {
                    return MaterializeConstant.createIntConstant(Long.parseLong(s));
                } catch (NumberFormatException e) {
                    return MaterializeConstant.createIntConstant(-1);
                }
            case TEXT:
                return this;
            default:
                return null;
            }
        }

        @Override
        public MaterializeDataType getExpressionType() {
            return MaterializeDataType.TEXT;
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

    public static class IntConstant extends MaterializeConstant {

        private final long val;

        public IntConstant(long val) {
            this.val = val;
        }

        @Override
        public String getTextRepresentation() {
            return String.valueOf(val);
        }

        @Override
        public MaterializeDataType getExpressionType() {
            return MaterializeDataType.INT;
        }

        @Override
        public long asInt() {
            return val;
        }

        @Override
        public boolean isInt() {
            return true;
        }

        @Override
        public MaterializeConstant isEquals(MaterializeConstant rightVal) {
            if (rightVal.isNull()) {
                return MaterializeConstant.createNullConstant();
            } else if (rightVal.isBoolean()) {
                return cast(MaterializeDataType.BOOLEAN).isEquals(rightVal);
            } else if (rightVal.isInt()) {
                return MaterializeConstant.createBooleanConstant(val == rightVal.asInt());
            } else if (rightVal.isString()) {
                return MaterializeConstant.createBooleanConstant(val == rightVal.cast(MaterializeDataType.INT).asInt());
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        protected MaterializeConstant isLessThan(MaterializeConstant rightVal) {
            if (rightVal.isNull()) {
                return MaterializeConstant.createNullConstant();
            } else if (rightVal.isInt()) {
                return MaterializeConstant.createBooleanConstant(val < rightVal.asInt());
            } else if (rightVal.isBoolean()) {
                throw new AssertionError(rightVal);
            } else if (rightVal.isString()) {
                return MaterializeConstant.createBooleanConstant(val < rightVal.cast(MaterializeDataType.INT).asInt());
            } else {
                throw new IgnoreMeException();
            }

        }

        @Override
        public MaterializeConstant cast(MaterializeDataType type) {
            switch (type) {
            case BOOLEAN:
                return MaterializeConstant.createBooleanConstant(val != 0);
            case INT:
                return this;
            case TEXT:
                return MaterializeConstant.createTextConstant(String.valueOf(val));
            default:
                return null;
            }
        }

        @Override
        public String getUnquotedTextRepresentation() {
            return getTextRepresentation();
        }

    }

    public static MaterializeConstant createNullConstant() {
        return new MaterializeNullConstant();
    }

    public String asString() {
        throw new UnsupportedOperationException(this.toString());
    }

    public boolean isString() {
        return false;
    }

    public static MaterializeConstant createIntConstant(long val) {
        return new IntConstant(val);
    }

    public static MaterializeConstant createBooleanConstant(boolean val) {
        return new BooleanConstant(val);
    }

    @Override
    public MaterializeConstant getExpectedValue() {
        return this;
    }

    public boolean isNull() {
        return false;
    }

    public boolean asBoolean() {
        throw new UnsupportedOperationException(this.toString());
    }

    public static MaterializeConstant createFalse() {
        return createBooleanConstant(false);
    }

    public static MaterializeConstant createTrue() {
        return createBooleanConstant(true);
    }

    public long asInt() {
        throw new UnsupportedOperationException(this.toString());
    }

    public boolean isBoolean() {
        return false;
    }

    public abstract MaterializeConstant isEquals(MaterializeConstant rightVal);

    public boolean isInt() {
        return false;
    }

    protected abstract MaterializeConstant isLessThan(MaterializeConstant rightVal);

    @Override
    public String toString() {
        return getTextRepresentation();
    }

    public abstract MaterializeConstant cast(MaterializeDataType type);

    public static MaterializeConstant createTextConstant(String string) {
        return new StringConstant(string);
    }

    public abstract static class MaterializeConstantBase extends MaterializeConstant {

        @Override
        public String getUnquotedTextRepresentation() {
            return null;
        }

        @Override
        public MaterializeConstant isEquals(MaterializeConstant rightVal) {
            return null;
        }

        @Override
        protected MaterializeConstant isLessThan(MaterializeConstant rightVal) {
            return null;
        }

        @Override
        public MaterializeConstant cast(MaterializeDataType type) {
            return null;
        }
    }

    public static class DecimalConstant extends MaterializeConstantBase {

        private final BigDecimal val;

        public DecimalConstant(BigDecimal val) {
            this.val = val;
        }

        @Override
        public String getTextRepresentation() {
            return String.valueOf(val);
        }

        @Override
        public MaterializeDataType getExpressionType() {
            return MaterializeDataType.DECIMAL;
        }

    }

    public static class FloatConstant extends MaterializeConstantBase {

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
        public MaterializeDataType getExpressionType() {
            return MaterializeDataType.FLOAT;
        }

    }

    public static class DoubleConstant extends MaterializeConstantBase {

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
        public MaterializeDataType getExpressionType() {
            return MaterializeDataType.FLOAT;
        }

    }

    public static class BitConstant extends MaterializeConstantBase {

        private final long val;

        public BitConstant(long val) {
            this.val = val;
        }

        @Override
        public String getTextRepresentation() {
            return String.format("%d", val);
        }

        @Override
        public MaterializeDataType getExpressionType() {
            return MaterializeDataType.BIT;
        }

    }

    public static MaterializeConstant createDecimalConstant(BigDecimal bigDecimal) {
        return new DecimalConstant(bigDecimal);
    }

    public static MaterializeConstant createFloatConstant(float val) {
        return new FloatConstant(val);
    }

    public static MaterializeConstant createDoubleConstant(double val) {
        return new DoubleConstant(val);
    }

    public static MaterializeExpression createBitConstant(long integer) {
        return new BitConstant(integer);
    }

}
