package sqlancer.yugabyte.ysql.ast;

import sqlancer.yugabyte.ysql.YSQLCompoundDataType;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLDataType;

public class YSQLCastOperation implements YSQLExpression {

    private final YSQLExpression expression;
    private final YSQLCompoundDataType type;

    public YSQLCastOperation(YSQLExpression expression, YSQLCompoundDataType type) {
        if (expression == null) {
            throw new AssertionError();
        }
        this.expression = expression;
        this.type = type;
    }

    @Override
    public YSQLDataType getExpressionType() {
        return type.getDataType();
    }

    @Override
    public YSQLConstant getExpectedValue() {
        YSQLConstant expectedValue = expression.getExpectedValue();
        if (expectedValue == null) {
            return null;
        }
        return expectedValue.cast(type.getDataType());
    }

    public YSQLExpression getExpression() {
        return expression;
    }

    public YSQLDataType getType() {
        return type.getDataType();
    }

    public YSQLCompoundDataType getCompoundType() {
        return type;
    }

}
