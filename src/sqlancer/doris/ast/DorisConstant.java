package sqlancer.doris.ast;

import sqlancer.common.ast.newast.Node;
import sqlancer.doris.DorisSchema.DorisDataType;
import sqlancer.doris.utils.DorisNumberUtils;

public abstract class DorisConstant implements Node<DorisExpression>, DorisExpression {

    private DorisConstant() {
    }

    public boolean isNull() {
        return false;
    }

    public boolean isInt() {
        return false;
    }

    public boolean isBoolean() {
        return false;
    }

    public boolean isString() {
        return false;
    }

    public boolean isFloat() {
        return false;
    }

    public boolean isNum() {
        // for INT, FLOAT, BOOLEAN
        return false;
    }

    public boolean isDate() {
        return false;
    }

    public boolean isDatetime() {
        return false;
    }

    public boolean asBoolean() {
        throw new UnsupportedOperationException(this.toString());
    }

    public long asInt() {
        throw new UnsupportedOperationException(this.toString());
    }

    public String asString() {
        throw new UnsupportedOperationException(this.toString());
    }

    public double asFloat() {
        throw new UnsupportedOperationException(this.toString());
    }

    public abstract DorisConstant cast(DorisDataType dataType);

    public abstract DorisConstant valueEquals(DorisConstant rightVal);

    public abstract DorisConstant valueLessThan(DorisConstant rightVal);

    public static class DorisNullConstant extends DorisConstant {

        @Override
        public String toString() {
            return "NULL";
        }

        @Override
        public boolean isNull() {
            return true;
        }

        @Override
        public DorisConstant cast(DorisDataType dataType) {
            return DorisConstant.createNullConstant();
        }

        @Override
        public DorisConstant valueEquals(DorisConstant rightVal) {
            return DorisConstant.createNullConstant();
        }

        @Override
        public DorisConstant valueLessThan(DorisConstant rightVal) {
            return DorisConstant.createNullConstant();
        }

        @Override
        public DorisDataType getExpectedType() {
            return DorisDataType.NULL;
        }
    }

    public static class DorisIntConstant extends DorisConstant {

        private final long value;

        public DorisIntConstant(long value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }

        public long getValue() {
            return value;
        }

        @Override
        public boolean isInt() {
            return true;
        }

        @Override
        public boolean isNum() {
            return true;
        }

        @Override
        public DorisConstant cast(DorisDataType dataType) {
            switch (dataType) {
            case INT:
                return this;
            case FLOAT:
            case DECIMAL:
                return new DorisFloatConstant(value);
            case VARCHAR:
                return new DorisTextConstant(String.valueOf(value));
            case BOOLEAN:
                return new DorisBooleanConstant(value != 0);
            default:
                return DorisConstant.createNullConstant();
            }
        }

        @Override
        public long asInt() {
            return value;
        }

        @Override
        public boolean asBoolean() {
            return value != 0;
        }

        @Override
        public double asFloat() {
            return value;
        }

        @Override
        public String asString() {
            return String.valueOf(value);
        }

        @Override
        public DorisConstant valueEquals(DorisConstant rightVal) {
            if (rightVal.isNull()) {
                return DorisConstant.createNullConstant();
            }
            if (rightVal.isNum()) {
                return DorisConstant.createBooleanConstant(value == rightVal.asFloat());
            }

            throw new AssertionError(rightVal);
        }

        @Override
        public DorisConstant valueLessThan(DorisConstant rightVal) {
            if (rightVal.isNull()) {
                return DorisConstant.createNullConstant();
            }
            if (rightVal.isNum()) {
                return DorisConstant.createBooleanConstant(value < rightVal.asFloat());
            }

            throw new AssertionError(rightVal);
        }

        @Override
        public DorisDataType getExpectedType() {
            return DorisDataType.INT;
        }

    }

    public static class DorisFloatConstant extends DorisConstant {

        private final double value;

        public DorisFloatConstant(double value) {
            this.value = value;
        }

        public double getValue() {
            return value;
        }

        @Override
        public boolean isFloat() {
            return true;
        }

        @Override
        public boolean isNum() {
            return true;
        }

        @Override
        public String toString() {
            if (value == Double.POSITIVE_INFINITY) {
                return "3.40282347e+38";
            } else if (value == Double.NEGATIVE_INFINITY) {
                return "-3.40282347e+38";
            }

            return String.valueOf(value);
        }

        @Override
        public DorisConstant cast(DorisDataType dataType) {
            switch (dataType) {
            case INT:
                return new DorisIntConstant((long) value);
            case FLOAT:
            case DECIMAL:
                return this;
            case VARCHAR:
                return new DorisTextConstant(String.valueOf(value));
            case BOOLEAN:
                return new DorisBooleanConstant(value >= 1);
            default:
                return null;
            }
        }

        @Override
        public double asFloat() {
            return value;
        }

        @Override
        public String asString() {
            return toString();
        }

        @Override
        public DorisConstant valueEquals(DorisConstant rightVal) {
            if (rightVal.isNull()) {
                return DorisConstant.createNullConstant();
            } else if (rightVal.isInt()) {
                return DorisConstant.createBooleanConstant(value == rightVal.asInt());
            } else if (rightVal.isFloat()) {
                return DorisConstant.createBooleanConstant(value < rightVal.asFloat());
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        public DorisConstant valueLessThan(DorisConstant rightVal) {
            if (rightVal.isNull()) {
                return DorisConstant.createNullConstant();
            } else if (rightVal.isInt()) {
                return DorisConstant.createBooleanConstant(value < rightVal.asInt());
            } else if (rightVal.isFloat()) {
                return DorisConstant.createBooleanConstant(value < rightVal.asFloat());
            } else {
                throw new AssertionError(rightVal);
            }
        }

    }

    public static class DorisTextConstant extends DorisConstant {

        private final String value;

        public DorisTextConstant(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        @Override
        public String toString() {
            return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'";
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
        public DorisConstant cast(DorisDataType dataType) {
            switch (dataType) {
            case INT:
                // Currently only supports conversion of int text to int, not float text, see
                // https://github.com/apache/doris/issues/18227
                if (DorisNumberUtils.isNumber(value)) {
                    long val = (long) Double.parseDouble(value);
                    return new DorisIntConstant(val);
                }
                return new DorisNullConstant();
            case FLOAT:
            case DECIMAL:
                if (DorisNumberUtils.isNumber(value)) {
                    return new DorisFloatConstant(Double.parseDouble(value));
                }
                return new DorisNullConstant();
            case DATE:
                if (DorisNumberUtils.isDate(value)) {
                    return new DorisDateConstant(value);
                }
                return new DorisNullConstant();
            case DATETIME:
                if (DorisNumberUtils.isDatetime(value)) {
                    return new DorisDatetimeConstant(value);
                }
                return new DorisNullConstant();
            case VARCHAR:
                return this;
            case BOOLEAN:
                if ("false".contentEquals(value.toLowerCase())) {
                    return new DorisBooleanConstant(false);
                }
                if ("true".contentEquals(value.toLowerCase())) {
                    return new DorisBooleanConstant(true);
                }
                return new DorisNullConstant();
            default:
                return new DorisNullConstant();
            }
        }

        @Override
        public DorisConstant valueEquals(DorisConstant rightVal) {
            if (rightVal.isNull()) {
                return DorisConstant.createNullConstant();
            }
            if (rightVal.isString()) {
                return DorisConstant.createBooleanConstant(value.contentEquals(rightVal.asString()));
            }
            if (DorisNumberUtils.isNumber(value) && rightVal.isNum()) {
                return DorisConstant.createBooleanConstant(Double.parseDouble(value) == rightVal.asFloat());
            }
            // Doris currently does not support judgment between string types and other types, such date, datetime
            return DorisConstant.createBooleanConstant(false);
        }

        @Override
        public DorisConstant valueLessThan(DorisConstant rightVal) {
            if (rightVal.isNull()) {
                return DorisConstant.createNullConstant();
            }
            if (rightVal.isString()) {
                return DorisConstant.createBooleanConstant(value.compareTo(rightVal.asString()) < 0);
            }
            if (DorisNumberUtils.isNumber(value) && rightVal.isNum()) {
                return DorisConstant.createBooleanConstant(Double.parseDouble(value) < rightVal.asFloat());
            }
            // Doris currently does not support judgment between string types and other types, such date, datetime
            return DorisConstant.createBooleanConstant(false);
        }

    }

    public static class DorisDateConstant extends DorisConstant {

        public String textRepr;

        public DorisDateConstant(long val) {
            textRepr = DorisNumberUtils.timestampToDateText(val);
        }

        public DorisDateConstant(String textRepr) {
            this.textRepr = textRepr;
        }

        public String getValue() {
            return textRepr;
        }

        @Override
        public String toString() {
            return String.format("DATE '%s'", textRepr);
        }

        @Override
        public String asString() {
            return textRepr;
        }

        @Override
        public DorisConstant cast(DorisDataType dataType) {
            switch (dataType) {
            case VARCHAR:
                return new DorisTextConstant(textRepr);
            case DATE:
                return this;
            case DATETIME:
                return new DorisDatetimeConstant(DorisNumberUtils.dateTextToDatetimeText(textRepr));
            default:
                return new DorisNullConstant();
            }
        }

        @Override
        public DorisConstant valueEquals(DorisConstant rightVal) {
            if (rightVal.isNull()) {
                return DorisConstant.createNullConstant();
            }
            if (rightVal.isDatetime() && rightVal.asString().contentEquals("CURRENT_TIMESTAMP")) {
                return DorisConstant.createBooleanConstant(false);
            }
            if (rightVal.isString() || rightVal.isDate() || rightVal.isDatetime()) {
                return DorisConstant.createBooleanConstant(DorisNumberUtils.dateEqual(textRepr, rightVal.asString()));
            }
            return DorisConstant.createBooleanConstant(false);
        }

        @Override
        public DorisConstant valueLessThan(DorisConstant rightVal) {
            if (rightVal.isNull()) {
                return DorisConstant.createNullConstant();
            }
            if (rightVal.isDatetime() && rightVal.asString().contentEquals("CURRENT_TIMESTAMP")) {
                return DorisConstant.createBooleanConstant(
                        DorisNumberUtils.dateLessThan(textRepr, DorisNumberUtils.getCurrentTimeText()));
            }
            if (rightVal.isString() || rightVal.isDate() || rightVal.isDatetime()) {
                return DorisConstant
                        .createBooleanConstant(DorisNumberUtils.dateLessThan(textRepr, rightVal.asString()));
            }
            return DorisConstant.createBooleanConstant(false);
        }

        @Override
        public boolean isDate() {
            return true;
        }
    }

    public static class DorisDatetimeConstant extends DorisConstant {

        public String textRepr;

        public DorisDatetimeConstant(long val) {
            textRepr = DorisNumberUtils.timestampToDatetimeText(val);
        }

        public DorisDatetimeConstant(String textRepr) {
            this.textRepr = textRepr;
        }

        public DorisDatetimeConstant() {
            textRepr = "CURRENT_TIMESTAMP";
        }

        public String getValue() {
            return textRepr;
        }

        @Override
        public String toString() {
            return String.format("TIMESTAMP '%s'", textRepr);
        }

        @Override
        public String asString() {
            return textRepr;
        }

        @Override
        public DorisConstant cast(DorisDataType dataType) {
            switch (dataType) {
            case VARCHAR:
                return new DorisTextConstant(textRepr);
            case DATE:
                return new DorisDatetimeConstant(DorisNumberUtils.datetimeTextToDateText(textRepr));
            case DATETIME:
                return this;
            default:
                return new DorisNullConstant();
            }
        }

        @Override
        public DorisConstant valueEquals(DorisConstant rightVal) {
            if (rightVal.isNull()) {
                return DorisConstant.createNullConstant();
            }
            if (rightVal.isDatetime() && (rightVal.asString().contentEquals("CURRENT_TIMESTAMP")
                    || textRepr.contentEquals("CURRENT_TIMESTAMP"))) {
                boolean isEq = rightVal.asString().contentEquals("CURRENT_TIMESTAMP")
                        && textRepr.contentEquals("CURRENT_TIMESTAMP");
                return DorisConstant.createBooleanConstant(isEq);
            }
            if (rightVal.isString() || rightVal.isDate() || rightVal.isDatetime()) {
                return DorisConstant
                        .createBooleanConstant(DorisNumberUtils.datetimeEqual(textRepr, rightVal.asString()));
            }
            return DorisConstant.createBooleanConstant(false);
        }

        @Override
        public DorisConstant valueLessThan(DorisConstant rightVal) {
            if (rightVal.isNull()) {
                return DorisConstant.createNullConstant();
            }
            if (rightVal.isDatetime() && (rightVal.asString().contentEquals("CURRENT_TIMESTAMP")
                    || textRepr.contentEquals("CURRENT_TIMESTAMP"))) {
                String leftText = textRepr;
                String rightText = rightVal.asString();
                if (leftText.contentEquals(rightText)) {
                    return DorisConstant.createBooleanConstant(false);
                }
                if (leftText.contentEquals("CURRENT_TIMESTAMP")) {
                    leftText = DorisNumberUtils.getCurrentTimeText();
                }
                if (rightText.contentEquals("CURRENT_TIMESTAMP")) {
                    rightText = DorisNumberUtils.getCurrentTimeText();
                }
                boolean lessThan = DorisNumberUtils.dateLessThan(leftText, rightText);
                return DorisConstant.createBooleanConstant(lessThan);
            }
            if (rightVal.isString() || rightVal.isDate() || rightVal.isDatetime()) {
                return DorisConstant
                        .createBooleanConstant(DorisNumberUtils.datetimeLessThan(textRepr, rightVal.asString()));
            }
            return DorisConstant.createBooleanConstant(false);
        }

        @Override
        public boolean isDatetime() {
            return true;
        }

    }

    public static class DorisBooleanConstant extends DorisConstant {

        private final boolean value;

        public DorisBooleanConstant(boolean value) {
            this.value = value;
        }

        public boolean getValue() {
            return value;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }

        @Override
        public String asString() {
            return toString();
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
        public boolean isNum() {
            return true;
        }

        @Override
        public DorisConstant cast(DorisDataType dataType) {
            switch (dataType) {
            case INT:
                return new DorisIntConstant(value ? 1 : 0);
            case FLOAT:
            case DECIMAL:
                return new DorisFloatConstant(value ? 1 : 0);
            case BOOLEAN:
                return this;
            case VARCHAR:
                return new DorisTextConstant(value ? "1" : "0");
            default:
                return null;
            }
        }

        @Override
        public DorisConstant valueEquals(DorisConstant rightVal) {
            if (rightVal.isNull()) {
                return DorisConstant.createNullConstant();
            }
            if (rightVal.isBoolean()) {
                return DorisConstant.createBooleanConstant(value == rightVal.asBoolean());
            }
            if (rightVal.isNum() || rightVal.isString() && DorisNumberUtils.isNumber(rightVal.asString())) {
                return DorisConstant.createBooleanConstant((value ? 1 : 0) == rightVal.asFloat());
            }
            throw new AssertionError(rightVal);
        }

        @Override
        public DorisConstant valueLessThan(DorisConstant rightVal) {
            if (rightVal.isNull()) {
                return DorisConstant.createNullConstant();
            }
            if (rightVal.isBoolean()) {
                return DorisConstant.createBooleanConstant(value == rightVal.asBoolean());
            }
            if (rightVal.isNum() || rightVal.isString() && DorisNumberUtils.isNumber(rightVal.asString())) {
                return DorisConstant.createBooleanConstant((value ? 1 : 0) == rightVal.asFloat());
            }
            throw new AssertionError(rightVal);
        }
    }

    public static DorisConstant createStringConstant(String text) {
        return new DorisTextConstant(text);
    }

    public static DorisConstant createFloatConstant(double val) {
        return new DorisFloatConstant(val);
    }

    public static DorisConstant createIntConstant(long val) {
        return new DorisIntConstant(val);
    }

    public static DorisConstant createNullConstant() {
        return new DorisNullConstant();
    }

    public static DorisConstant createBooleanConstant(boolean val) {
        return new DorisBooleanConstant(val);
    }

    public static DorisConstant createDateConstant(long integer) {
        return new DorisDateConstant(integer);
    }

    public static DorisConstant createDateConstant(String date) {
        return new DorisDateConstant(date);
    }

    public static DorisConstant createDatetimeConstant(long integer) {
        return new DorisDatetimeConstant(integer);
    }

    public static DorisConstant createDatetimeConstant(String datetime) {
        return new DorisDatetimeConstant(datetime);
    }

    public static DorisConstant createDatetimeConstant() {
        // use CURRENT_TIMESTAMP
        return new DorisDatetimeConstant();
    }

}
