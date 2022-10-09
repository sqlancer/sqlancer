package sqlancer.yugabyte.ysql.ast;

import java.util.List;

import sqlancer.yugabyte.ysql.YSQLSchema.YSQLDataType;

public class YSQLInOperation implements YSQLExpression {

    private final YSQLExpression expr;
    private final List<YSQLExpression> listElements;
    private final boolean isTrue;

    public YSQLInOperation(YSQLExpression expr, List<YSQLExpression> listElements, boolean isTrue) {
        this.expr = expr;
        this.listElements = listElements;
        this.isTrue = isTrue;
    }

    public YSQLExpression getExpr() {
        return expr;
    }

    public List<YSQLExpression> getListElements() {
        return listElements;
    }

    public boolean isTrue() {
        return isTrue;
    }

    @Override
    public YSQLDataType getExpressionType() {
        return YSQLDataType.BOOLEAN;
    }

    @Override
    public YSQLConstant getExpectedValue() {
        YSQLConstant leftValue = expr.getExpectedValue();
        if (leftValue == null) {
            return null;
        }
        if (leftValue.isNull()) {
            return YSQLConstant.createNullConstant();
        }
        boolean isNull = false;
        for (YSQLExpression expr : getListElements()) {
            YSQLConstant rightExpectedValue = expr.getExpectedValue();
            if (rightExpectedValue == null) {
                return null;
            }
            if (rightExpectedValue.isNull()) {
                isNull = true;
            } else if (rightExpectedValue.isEquals(this.expr.getExpectedValue()).isBoolean()
                    && rightExpectedValue.isEquals(this.expr.getExpectedValue()).asBoolean()) {
                return YSQLConstant.createBooleanConstant(isTrue);
            }
        }

        if (isNull) {
            return YSQLConstant.createNullConstant();
        } else {
            return YSQLConstant.createBooleanConstant(!isTrue);
        }
    }
}
