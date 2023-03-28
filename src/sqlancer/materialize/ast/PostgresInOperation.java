package sqlancer.postgres.ast;

import java.util.List;

import sqlancer.postgres.PostgresSchema.PostgresDataType;

public class PostgresInOperation implements PostgresExpression {

    private final PostgresExpression expr;
    private final List<PostgresExpression> listElements;
    private final boolean isTrue;

    public PostgresInOperation(PostgresExpression expr, List<PostgresExpression> listElements, boolean isTrue) {
        this.expr = expr;
        this.listElements = listElements;
        this.isTrue = isTrue;
    }

    public PostgresExpression getExpr() {
        return expr;
    }

    public List<PostgresExpression> getListElements() {
        return listElements;
    }

    @Override
    public PostgresConstant getExpectedValue() {
        PostgresConstant leftValue = expr.getExpectedValue();
        if (leftValue == null) {
            return null;
        }
        if (leftValue.isNull()) {
            return PostgresConstant.createNullConstant();
        }
        boolean isNull = false;
        for (PostgresExpression expr : getListElements()) {
            PostgresConstant rightExpectedValue = expr.getExpectedValue();
            if (rightExpectedValue == null) {
                return null;
            }
            if (rightExpectedValue.isNull()) {
                isNull = true;
            } else if (rightExpectedValue.isEquals(this.expr.getExpectedValue()).isBoolean()
                    && rightExpectedValue.isEquals(this.expr.getExpectedValue()).asBoolean()) {
                return PostgresConstant.createBooleanConstant(isTrue);
            }
        }

        if (isNull) {
            return PostgresConstant.createNullConstant();
        } else {
            return PostgresConstant.createBooleanConstant(!isTrue);
        }
    }

    public boolean isTrue() {
        return isTrue;
    }

    @Override
    public PostgresDataType getExpressionType() {
        return PostgresDataType.BOOLEAN;
    }
}
