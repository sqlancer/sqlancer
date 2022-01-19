package sqlancer.oceanbase.ast;

import java.math.BigDecimal;
import java.math.BigInteger;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.oceanbase.OceanBaseSchema.OceanBaseDataType;
import sqlancer.oceanbase.ast.OceanBaseCastOperation.CastType;

public abstract class OceanBaseConstant implements OceanBaseExpression {

    public boolean isInt() {
        return false;
    }

    public boolean isNull() {
        return false;
    }

    public boolean isDouble() {
        return false;
    }

    public boolean isEmpty() {
        return false;
    }

    public abstract static class OceanBaseNoPQSConstant extends OceanBaseConstant {

        @Override
        public boolean asBooleanNotNull() {
            throw throwException();
        }

        private RuntimeException throwException() {
            throw new UnsupportedOperationException("not applicable for PQS evaluation!");
        }

        @Override
        public OceanBaseConstant isEquals(OceanBaseConstant rightVal) {
            return null;
        }

        @Override
        public OceanBaseConstant castAs(CastType type) {
            throw throwException();
        }

        @Override
        public String castAsString() {
            throw throwException();

        }

        @Override
        public OceanBaseConstant castAsDouble() {
            throw throwException();
        }

        @Override
        public OceanBaseDataType getType() {
            throw throwException();
        }

        @Override
        protected OceanBaseConstant isLessThan(OceanBaseConstant rightVal) {
            throw throwException();
        }

    }

    public static class OceanBaseDoubleConstant extends OceanBaseNoPQSConstant {

        private final double val;

        public OceanBaseDoubleConstant(double val) {
            this.val = val;
            if (Double.isInfinite(val) || Double.isNaN(val)) {
                throw new IgnoreMeException();
            }
        }

        @Override
        public String getTextRepresentation() {
            return String.valueOf(val);
        }

        @Override
        public double getDouble() {
            return this.val;
        }

        @Override
        public long getInt() {
            return (long) val;
        }

        @Override
        public boolean asBooleanNotNull() {
            return Double.compare(Math.abs(val), 0.0) != 0;
        }

        @Override
        public OceanBaseConstant castAs(CastType type) {
            if (type == CastType.SIGNED) {
                long value = (long) val;
                if (val - value >= 0.5) {
                    value = value + 1;
                }
                return new OceanBaseIntConstant(value, true);
            } else if (type == CastType.UNSIGNED) {
                long value = (long) val;
                if (val - value >= 0.5) {
                    value = value + 1;
                }
                return new OceanBaseIntConstant(value, false);
            } else {
                throw new AssertionError();
            }
        }

        @Override
        public String castAsString() {
            return String.valueOf(new BigDecimal(val)); // select IFNULL(1.713591018E9, '11') -> 1713591018
        }

        @Override
        public boolean isDouble() {
            return true;
        }

        @Override
        protected OceanBaseConstant isLessThan(OceanBaseConstant rightVal) {
            if (rightVal.isNull()) {
                return OceanBaseConstant.createNullConstant();
            } else if (rightVal instanceof OceanBaseIntConstant) {
                return OceanBaseConstant.createBoolean(val < rightVal.getInt());
            } else if (rightVal instanceof OceanBaseDoubleConstant) {
                return OceanBaseConstant.createBoolean(val < rightVal.getDouble());
            } else if (rightVal instanceof OceanBaseTextConstant) {
                return isLessThan(rightVal.castAsDouble());
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        public OceanBaseConstant isEquals(OceanBaseConstant rightVal) {
            if (rightVal.isNull()) {
                return OceanBaseConstant.createNullConstant();
            } else if (rightVal instanceof OceanBaseIntConstant) {
                return OceanBaseConstant.createBoolean(val == rightVal.getInt());
            } else if (rightVal instanceof OceanBaseDoubleConstant) {
                return OceanBaseConstant.createBoolean(val == rightVal.getDouble());
            } else if (rightVal instanceof OceanBaseTextConstant) {
                return isEquals(rightVal.castAsDouble());
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        public OceanBaseDataType getType() {
            return OceanBaseDataType.DOUBLE;
        }

    }

    public static class OceanBaseTextConstant extends OceanBaseConstant {

        private final String value;
        private final boolean singleQuotes;

        public OceanBaseTextConstant(String value) {
            this.value = value;
            singleQuotes = Randomly.getBoolean();

        }

        private void checkIfSmallFloatingPointText() {
            boolean isSmallFloatingPointText = isString() && asBooleanNotNull()
                    && castAs(CastType.SIGNED).getInt() == 0;
            if (isSmallFloatingPointText) {
                throw new IgnoreMeException();
            }
        }

        @Override
        public boolean isNull() {
            return value.equalsIgnoreCase("NULL");
        }

        @Override
        public boolean isEmpty() {
            // "" " "
            if (value.length() == 0) {
                return true;
            } else {
                for (int i = 0; i < value.length(); i++) {
                    String sub = value.substring(i, i + 1);
                    if (!sub.equals(" ")) {
                        return false;
                    }
                }
                return true;
            }
        }

        @Override
        public boolean asBooleanNotNull() {
            for (int i = value.length(); i >= 1; i--) {
                try {
                    String substring = value.substring(0, i);
                    Double val = Double.valueOf(substring);
                    return val != 0 && !Double.isNaN(val);
                } catch (NumberFormatException e) {
                    // ignore
                }
            }
            return false;
        }

        @Override
        public String getTextRepresentation() {
            StringBuilder sb = new StringBuilder();
            String quotes = singleQuotes ? "'" : "\"";
            sb.append(quotes);
            String text = value.replace(quotes, quotes + quotes).replace("\\", "\\\\");
            sb.append(text);
            sb.append(quotes);
            return sb.toString();
        }

        @Override
        public OceanBaseConstant isEquals(OceanBaseConstant rightVal) {
            if (isNull() || rightVal.isNull()) {
                return OceanBaseConstant.createNullConstant();
            } else if (rightVal.isInt()) {
                checkIfSmallFloatingPointText();
                if (asBooleanNotNull()) {
                    throw new IgnoreMeException();
                }
                return castAs(CastType.SIGNED).isEquals(rightVal);
            } else if (rightVal instanceof OceanBaseDoubleConstant) {
                return castAsDouble().isEquals(rightVal);
            } else if (rightVal.isString()) {
                if (isEmpty() && rightVal.isEmpty()) {
                    return OceanBaseConstant.createBoolean(true);
                } else {
                    return OceanBaseConstant.createBoolean(value.equalsIgnoreCase(rightVal.getString()));
                }
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        public String getString() {
            return value;
        }

        @Override
        public boolean isString() {
            return true;
        }

        @Override
        public OceanBaseConstant castAs(CastType type) {
            if (isNull()) {
                return OceanBaseConstant.createNullConstant();
            }
            if (type == CastType.SIGNED || type == CastType.UNSIGNED) {
                String value = this.value;
                while (value.startsWith(" ") || value.startsWith("\t") || value.startsWith("\n")) {
                    if (value.startsWith("\n")) {
                        throw new IgnoreMeException();
                    }
                    value = value.substring(1);
                }
                for (int i = value.length(); i >= 1; i--) {
                    try {
                        String substring = value.substring(0, i);
                        long val = Long.parseLong(substring);
                        return OceanBaseConstant.createIntConstant(val, type == CastType.SIGNED);
                    } catch (NumberFormatException e) {
                    }
                }
                return OceanBaseConstant.createIntConstant(0, type == CastType.SIGNED);
            } else {
                throw new AssertionError();
            }
        }

        @Override
        public OceanBaseConstant castAsDouble() {
            String value = this.value;
            while (value.startsWith(" ") || value.startsWith("\t") || value.startsWith("\n")) {
                if (value.startsWith("\n")) {
                    throw new IgnoreMeException();
                }
                value = value.substring(1);
            }
            for (int i = value.length(); i >= 1; i--) {
                try {
                    String substring = value.substring(0, i);
                    double val = Double.parseDouble(substring);
                    return OceanBaseConstant.createDoubleConstant(val);
                } catch (NumberFormatException e) {
                }
            }
            return OceanBaseConstant.createIntConstant(0);
        }

        @Override
        public String castAsString() {
            return value;
        }

        @Override
        public OceanBaseDataType getType() {
            return OceanBaseDataType.VARCHAR;
        }

        @Override
        protected OceanBaseConstant isLessThan(OceanBaseConstant rightVal) {
            if (isNull() || rightVal.isNull()) {
                return OceanBaseConstant.createNullConstant();
            } else if (rightVal.isInt()) {
                if (asBooleanNotNull()) {
                    throw new IgnoreMeException();
                }
                checkIfSmallFloatingPointText();
                return castAs(rightVal.isSigned() ? CastType.SIGNED : CastType.UNSIGNED).isLessThan(rightVal);
            } else if (rightVal instanceof OceanBaseDoubleConstant) {
                return castAsDouble().isLessThan(rightVal);
            } else if (rightVal.isString()) {
                throw new IgnoreMeException();
            } else {
                throw new AssertionError(rightVal);
            }
        }
    }

    public static class OceanBaseIntConstant extends OceanBaseConstant {

        private final long value;
        private final String stringRepresentation;
        private final boolean isSigned;

        public OceanBaseIntConstant(long value, boolean isSigned) {
            this.value = value;
            this.isSigned = isSigned;
            if (value == 0 && Randomly.getBoolean()) {
                stringRepresentation = "FALSE";
            } else if (value == 1 && Randomly.getBoolean()) {
                stringRepresentation = "TRUE";
            } else {
                if (isSigned) {
                    stringRepresentation = String.valueOf(value);
                } else {
                    stringRepresentation = Long.toUnsignedString(value);
                }
            }
        }

        public OceanBaseIntConstant(long value, String stringRepresentation) {
            this.value = value;
            this.stringRepresentation = stringRepresentation;
            isSigned = true;
        }

        @Override
        public boolean isInt() {
            return true;
        }

        @Override
        public long getInt() {
            return value;
        }

        @Override
        public boolean asBooleanNotNull() {
            return value != 0;
        }

        @Override
        public String getTextRepresentation() {
            return stringRepresentation;
        }

        @Override
        public OceanBaseConstant isEquals(OceanBaseConstant rightVal) {
            if (rightVal.isInt()) {
                return OceanBaseConstant.createBoolean(new BigInteger(getStringRepr())
                        .compareTo(new BigInteger(((OceanBaseIntConstant) rightVal).getStringRepr())) == 0);
            } else if (rightVal.isNull()) {
                return OceanBaseConstant.createNullConstant();
            } else if (rightVal.isString()) {
                if (rightVal.asBooleanNotNull()) {
                    throw new IgnoreMeException();
                }
                return isEquals(rightVal.castAs(CastType.SIGNED));
            } else if (rightVal instanceof OceanBaseDoubleConstant) {
                return OceanBaseConstant.createBoolean(value == rightVal.getDouble());
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        public OceanBaseConstant castAs(CastType type) {
            if (type == CastType.SIGNED) {
                return new OceanBaseIntConstant(value, true);
            } else if (type == CastType.UNSIGNED) {
                return new OceanBaseIntConstant(value, false);
            } else {
                throw new AssertionError();
            }
        }

        @Override
        public String castAsString() {
            if (isSigned) {
                return String.valueOf(value);
            } else {
                return Long.toUnsignedString(value);
            }
        }

        @Override
        public OceanBaseConstant castAsDouble() {
            return this;
        }

        @Override
        public OceanBaseDataType getType() {
            return OceanBaseDataType.INT;
        }

        @Override
        public boolean isSigned() {
            return isSigned;
        }

        private String getStringRepr() {
            if (isSigned) {
                return String.valueOf(value);
            } else {
                return Long.toUnsignedString(value);
            }
        }

        @Override
        protected OceanBaseConstant isLessThan(OceanBaseConstant rightVal) {
            if (rightVal.isInt()) {
                long intVal = rightVal.getInt();
                if (isSigned && rightVal.isSigned()) {
                    return OceanBaseConstant.createBoolean(value < intVal);
                } else {
                    return OceanBaseConstant.createBoolean(new BigInteger(getStringRepr())
                            .compareTo(new BigInteger(((OceanBaseIntConstant) rightVal).getStringRepr())) < 0);
                }
            } else if (rightVal.isNull()) {
                return OceanBaseConstant.createNullConstant();
            } else if (rightVal.isString()) {
                if (rightVal.asBooleanNotNull()) {
                    throw new IgnoreMeException();
                }
                return isLessThan(rightVal.castAs(isSigned ? CastType.SIGNED : CastType.UNSIGNED));
            } else if (rightVal instanceof OceanBaseDoubleConstant) {
                return OceanBaseConstant.createBoolean(value < rightVal.getDouble());
            } else {
                throw new AssertionError(rightVal);
            }
        }

    }

    public static class OceanBaseNullConstant extends OceanBaseConstant {

        @Override
        public boolean isNull() {
            return true;
        }

        @Override
        public boolean asBooleanNotNull() {
            throw new UnsupportedOperationException(this.toString());
        }

        @Override
        public String getTextRepresentation() {
            return "NULL";
        }

        @Override
        public OceanBaseConstant isEquals(OceanBaseConstant rightVal) {
            return OceanBaseConstant.createNullConstant();
        }

        @Override
        public OceanBaseConstant castAs(CastType type) {
            return this;
        }

        @Override
        public String castAsString() {
            return "NULL";
        }

        @Override
        public OceanBaseConstant castAsDouble() {
            return this;
        }

        @Override
        public OceanBaseDataType getType() {
            return null;
        }

        @Override
        protected OceanBaseConstant isLessThan(OceanBaseConstant rightVal) {
            return this;
        }

    }

    public long getInt() {
        throw new UnsupportedOperationException();
    }

    public double getDouble() {
        throw new UnsupportedOperationException();
    }

    public boolean isSigned() {
        return false;
    }

    public String getString() {
        throw new UnsupportedOperationException();
    }

    public boolean isString() {
        return false;
    }

    public static OceanBaseConstant createNullConstant() {
        return new OceanBaseNullConstant();
    }

    public static OceanBaseConstant createIntConstant(long value) {
        return new OceanBaseIntConstant(value, true);
    }

    public static OceanBaseConstant createIntConstant(long value, boolean signed) {
        return new OceanBaseIntConstant(value, signed);
    }

    public static OceanBaseConstant createUnsignedIntConstant(long value) {
        return new OceanBaseIntConstant(value, false);
    }

    public static OceanBaseConstant createIntConstantNotAsBoolean(long value) {
        return new OceanBaseIntConstant(value, String.valueOf(value));
    }

    public static OceanBaseConstant createDoubleConstant(double value) {
        return new OceanBaseDoubleConstant(value);
    }

    @Override
    public OceanBaseConstant getExpectedValue() {
        return this;
    }

    public abstract boolean asBooleanNotNull();

    public abstract String getTextRepresentation();

    public static OceanBaseConstant createFalse() {
        return OceanBaseConstant.createIntConstant(0);
    }

    public static OceanBaseConstant createBoolean(boolean isTrue) {
        return OceanBaseConstant.createIntConstant(isTrue ? 1 : 0);
    }

    public static OceanBaseConstant createTrue() {
        return OceanBaseConstant.createIntConstant(1);
    }

    @Override
    public String toString() {
        return getTextRepresentation();
    }

    public abstract OceanBaseConstant isEquals(OceanBaseConstant rightVal);

    public abstract OceanBaseConstant castAs(CastType type);

    public abstract String castAsString();

    public abstract OceanBaseConstant castAsDouble();

    public static OceanBaseConstant createStringConstant(String string) {
        return new OceanBaseTextConstant(string);
    }

    public abstract OceanBaseDataType getType();

    protected abstract OceanBaseConstant isLessThan(OceanBaseConstant rightVal);

}
