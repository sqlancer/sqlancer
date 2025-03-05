package sqlancer.mysql.ast;

import sqlancer.mysql.MySQLSchema;

public class MySQLBooleanConstant extends MySQLConstant {
    private final boolean value;

    public MySQLBooleanConstant(boolean value) {
        this.value = value;
    }

    public static MySQLBooleanConstant createTrue() {
        return new MySQLBooleanConstant(true);
    }

    public static MySQLBooleanConstant createFalse() {
        return new MySQLBooleanConstant(false);
    }

    @Override
    public boolean isBoolean() {
        return true;
    }

    @Override
    public boolean asBooleanNotNull() {
        return value;
    }

    @Override
    public MySQLConstant castAs(MySQLCastOperation.CastType type) {
        switch (type) {
            case INT:
                return MySQLConstant.createIntConstant(value ? 1 : 0);
            case TEXT:
                return MySQLConstant.createStringConstant(value ? "TRUE" : "FALSE");
            case BOOLEAN:
                return this; // Already boolean
            default:
                throw new UnsupportedOperationException("Unsupported cast type: " + type);
        }
    }

    @Override
    public String castAsString() {
        return value ? "TRUE" : "FALSE";
    }

    @Override
    public MySQLSchema.MySQLDataType getType() {
        return MySQLSchema.MySQLDataType.BOOLEAN;
    }

    @Override
    public String getTextRepresentation() {
        return value ? "TRUE" : "FALSE";
    }

    @Override
    public MySQLConstant isEquals(MySQLConstant rightVal) {
        if (rightVal.isBoolean()) {
            return MySQLConstant.createBoolean(this.value == rightVal.asBooleanNotNull());
        }
        throw new UnsupportedOperationException("Cannot compare boolean with non-boolean values.");
    }


    @Override
    protected MySQLConstant isLessThan(MySQLConstant rightVal) {
        if (rightVal.isBoolean()) {
            return MySQLConstant.createBoolean(!this.value && rightVal.asBooleanNotNull());
        }
        throw new UnsupportedOperationException("Cannot compare boolean with non-boolean values.");
    }


}