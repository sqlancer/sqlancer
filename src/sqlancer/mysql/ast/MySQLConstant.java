package sqlancer.mysql.ast;

import java.math.BigInteger;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.mysql.MySQLSchema.MySQLDataType;
import sqlancer.mysql.ast.MySQLCastOperation.CastType;

public abstract class MySQLConstant implements MySQLExpression {

    public boolean isInt() {
        return false;
    }

    public boolean isNull() {
        return false;
    }

    public abstract static class MySQLNoPQSConstant extends MySQLConstant {

        @Override
        public boolean asBooleanNotNull() {
            throw throwException();
        }

        private RuntimeException throwException() {
            throw new UnsupportedOperationException("not applicable for PQS evaluation!");
        }

        @Override
        public MySQLConstant isEquals(MySQLConstant rightVal) {
            return null;
        }

        @Override
        public MySQLConstant castAs(CastType type) {
            throw throwException();
        }

        @Override
        public String castAsString() {
            throw throwException();

        }

        @Override
        public MySQLDataType getType() {
            throw throwException();
        }

        @Override
        protected MySQLConstant isLessThan(MySQLConstant rightVal) {
            throw throwException();
        }

    }

    public static class MySQLDoubleConstant extends MySQLNoPQSConstant {

        private final double val;

        public MySQLDoubleConstant(double val) {
            this.val = val;
            if (Double.isInfinite(val) || Double.isNaN(val)) {
                // seems to not be supported by MySQL
                throw new IgnoreMeException();
            }
        }

        @Override
        public String getTextRepresentation() {
            return String.valueOf(val);
        }

    }

    public static class MySQLTextConstant extends MySQLConstant {

        private final String value;
        private final boolean singleQuotes;

        public MySQLTextConstant(String value) {
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
        public boolean asBooleanNotNull() {
            // TODO implement as cast
            for (int i = value.length(); i >= 0; i--) {
                try {
                    String substring = value.substring(0, i);
                    Double val = Double.valueOf(substring);
                    return val != 0 && !Double.isNaN(val);
                } catch (NumberFormatException e) {
                    // ignore
                }
            }
            return false;
            // return castAs(CastType.SIGNED).getInt() != 0;
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
        public MySQLConstant isEquals(MySQLConstant rightVal) {
            if (rightVal.isNull()) {
                return MySQLConstant.createNullConstant();
            } else if (rightVal.isInt()) {
                checkIfSmallFloatingPointText();
                if (asBooleanNotNull()) {
                    // TODO support SELECT .123 = '.123'; by converting to floating point
                    throw new IgnoreMeException();
                }
                return castAs(CastType.SIGNED).isEquals(rightVal);
            } else if (rightVal.isString()) {
                return MySQLConstant.createBoolean(value.equalsIgnoreCase(rightVal.getString()));
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
        public MySQLConstant castAs(CastType type) {
            if (type == CastType.SIGNED || type == CastType.UNSIGNED) {
                String value = this.value;
                while (value.startsWith(" ") || value.startsWith("\t") || value.startsWith("\n")) {
                    if (value.startsWith("\n")) {
                        /* workaround for https://bugs.mysql.com/bug.php?id=96294 */
                        throw new IgnoreMeException();
                    }
                    value = value.substring(1);
                }
                for (int i = value.length(); i >= 0; i--) {
                    try {
                        String substring = value.substring(0, i);
                        long val = Long.parseLong(substring);
                        return MySQLConstant.createIntConstant(val, type == CastType.SIGNED);
                    } catch (NumberFormatException e) {
                        // ignore
                    }
                }
                return MySQLConstant.createIntConstant(0, type == CastType.SIGNED);
            } else {
                throw new AssertionError();
            }
        }

        @Override
        public String castAsString() {
            return value;
        }

        @Override
        public MySQLDataType getType() {
            return MySQLDataType.VARCHAR;
        }

        @Override
        protected MySQLConstant isLessThan(MySQLConstant rightVal) {
            if (rightVal.isNull()) {
                return MySQLConstant.createNullConstant();
            } else if (rightVal.isInt()) {
                if (asBooleanNotNull()) {
                    // TODO uspport floating point
                    throw new IgnoreMeException();
                }
                checkIfSmallFloatingPointText();
                return castAs(rightVal.isSigned() ? CastType.SIGNED : CastType.UNSIGNED).isLessThan(rightVal);
            } else if (rightVal.isString()) {
                // unexpected result for '-' < "!";
                // return
                // MySQLConstant.createBoolean(value.compareToIgnoreCase(rightVal.getString()) <
                // 0);
                throw new IgnoreMeException();
            } else {
                throw new AssertionError(rightVal);
            }
        }

    }

    public static class MySQLIntConstant extends MySQLConstant {

        private final long value;
        private final String stringRepresentation;
        private final boolean isSigned;

        public MySQLIntConstant(long value, boolean isSigned) {
            this.value = value;
            this.isSigned = isSigned;
            if (isSigned) {
                stringRepresentation = String.valueOf(value);
            } else {
                stringRepresentation = Long.toUnsignedString(value);
            }
        }

        public MySQLIntConstant(long value, String stringRepresentation) {
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
        public MySQLConstant isEquals(MySQLConstant rightVal) {
            if (rightVal.isInt()) {
                return MySQLConstant.createBoolean(new BigInteger(getStringRepr())
                        .compareTo(new BigInteger(((MySQLIntConstant) rightVal).getStringRepr())) == 0);
            } else if (rightVal.isNull()) {
                return MySQLConstant.createNullConstant();
            } else if (rightVal.isString()) {
                if (rightVal.asBooleanNotNull()) {
                    // TODO support SELECT .123 = '.123'; by converting to floating point
                    throw new IgnoreMeException();
                }
                return isEquals(rightVal.castAs(CastType.SIGNED));
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        public MySQLConstant castAs(CastType type) {
            if (type == CastType.SIGNED) {
                return new MySQLIntConstant(value, true);
            } else if (type == CastType.UNSIGNED) {
                return new MySQLIntConstant(value, false);
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
        public MySQLDataType getType() {
            return MySQLDataType.INT;
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
        protected MySQLConstant isLessThan(MySQLConstant rightVal) {
            if (rightVal.isInt()) {
                long intVal = rightVal.getInt();
                if (isSigned && rightVal.isSigned()) {
                    return MySQLConstant.createBoolean(value < intVal);
                } else {
                    return MySQLConstant.createBoolean(new BigInteger(getStringRepr())
                            .compareTo(new BigInteger(((MySQLIntConstant) rightVal).getStringRepr())) < 0);
                    // return MySQLConstant.createBoolean(Long.compareUnsigned(value, intVal) < 0);
                }
            } else if (rightVal.isNull()) {
                return MySQLConstant.createNullConstant();
            } else if (rightVal.isString()) {
                if (rightVal.asBooleanNotNull()) {
                    // TODO support float
                    throw new IgnoreMeException();
                }
                return isLessThan(rightVal.castAs(isSigned ? CastType.SIGNED : CastType.UNSIGNED));
            } else {
                throw new AssertionError(rightVal);
            }
        }

    }

    public static class MySQLNullConstant extends MySQLConstant {

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
        public MySQLConstant isEquals(MySQLConstant rightVal) {
            return MySQLConstant.createNullConstant();
        }

        @Override
        public MySQLConstant castAs(CastType type) {
            return this;
        }

        @Override
        public String castAsString() {
            return "NULL";
        }

        @Override
        public MySQLDataType getType() {
            return null;
        }

        @Override
        protected MySQLConstant isLessThan(MySQLConstant rightVal) {
            return this;
        }

    }

    public long getInt() {
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

    public static MySQLConstant createNullConstant() {
        return new MySQLNullConstant();
    }

    public static MySQLConstant createIntConstant(long value) {
        return new MySQLIntConstant(value, true);
    }

    public static MySQLConstant createIntConstant(long value, boolean signed) {
        return new MySQLIntConstant(value, signed);
    }

    public static MySQLConstant createUnsignedIntConstant(long value) {
        return new MySQLIntConstant(value, false);
    }

    public static MySQLConstant createIntConstantNotAsBoolean(long value) {
        return new MySQLIntConstant(value, String.valueOf(value));
    }

    @Override
    public MySQLConstant getExpectedValue() {
        return this;
    }

    public abstract boolean asBooleanNotNull();

    public abstract String getTextRepresentation();

    public static MySQLConstant createFalse() {
        return MySQLConstant.createIntConstant(0);
    }

    public static MySQLConstant createBoolean(boolean isTrue) {
        return MySQLConstant.createIntConstant(isTrue ? 1 : 0);
    }

    public static MySQLConstant createTrue() {
        return MySQLConstant.createIntConstant(1);
    }

    @Override
    public String toString() {
        return getTextRepresentation();
    }

    public abstract MySQLConstant isEquals(MySQLConstant rightVal);

    public abstract MySQLConstant castAs(CastType type);

    public abstract String castAsString();

    public static MySQLConstant createStringConstant(String string) {
        return new MySQLTextConstant(string);
    }

    public abstract MySQLDataType getType();

    protected abstract MySQLConstant isLessThan(MySQLConstant rightVal);

}
