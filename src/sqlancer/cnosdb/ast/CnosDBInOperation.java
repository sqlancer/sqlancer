package sqlancer.cnosdb.ast;

import sqlancer.cnosdb.CnosDBSchema.CnosDBDataType;

import java.util.List;

public class CnosDBInOperation implements CnosDBExpression {

    private final CnosDBExpression expr;
    private final List<CnosDBExpression> listElements;
    private final boolean isTrue;

    public CnosDBInOperation(CnosDBExpression expr, List<CnosDBExpression> listElements, boolean isTrue) {
        this.expr = expr;
        this.listElements = listElements;
        this.isTrue = isTrue;
    }

    public CnosDBExpression getExpr() {
        return expr;
    }

    public List<CnosDBExpression> getListElements() {
        return listElements;
    }

    @Override
    public CnosDBConstant getExpectedValue() {
        CnosDBConstant leftValue = expr.getExpectedValue();
        if (leftValue == null) {
            return null;
        }
        if (leftValue.isNull()) {
            return CnosDBConstant.createNullConstant();
        }
        boolean isNull = false;
        for (CnosDBExpression expr : getListElements()) {
            CnosDBConstant rightExpectedValue = expr.getExpectedValue();
            if (rightExpectedValue == null) {
                return null;
            }
            if (rightExpectedValue.isNull()) {
                isNull = true;
            } else if (rightExpectedValue.isEquals(this.expr.getExpectedValue()).isBoolean()
                    && rightExpectedValue.isEquals(this.expr.getExpectedValue()).asBoolean()) {
                return CnosDBConstant.createBooleanConstant(isTrue);
            }
        }

        if (isNull) {
            return CnosDBConstant.createNullConstant();
        } else {
            return CnosDBConstant.createBooleanConstant(!isTrue);
        }
    }

    public boolean isTrue() {
        return isTrue;
    }

    @Override
    public CnosDBDataType getExpressionType() {
        return CnosDBDataType.BOOLEAN;
    }
}
