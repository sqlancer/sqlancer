package sqlancer.yugabyte.ysql.ast;

import sqlancer.common.schema.AbstractCastOperation;
import sqlancer.yugabyte.ysql.YSQLCompoundDataType;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLDataType;

public class YSQLCastOperation implements YSQLExpression, AbstractCastOperation<YSQLExpression, YSQLDataType> {

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

    @Override
    public YSQLExpression getExpression() {
        return expression;
    }

    public YSQLDataType getType() {
        return type.getDataType();
    }

    @Override
    public YSQLCompoundDataType getCompoundType() {
        return type;
    }

}
