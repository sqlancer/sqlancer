package sqlancer.doris.ast;

import sqlancer.doris.DorisSchema.DorisCompositeDataType;
import sqlancer.doris.DorisSchema.DorisDataType;

public class DorisCastOperation implements DorisExpression {
    DorisExpression expr;
    DorisDataType type;

    public DorisCastOperation(DorisExpression expr, DorisCompositeDataType type) {
        this.expr = expr;
        this.type = type.getPrimitiveDataType();
    }

    public DorisCastOperation(DorisExpression expr, DorisDataType type) {
        this.expr = expr;
        this.type = type;
    }

    public DorisExpression getExpr() {
        return expr;
    }

    public DorisExpression getExpression() {
        return expr;
    }

    public DorisDataType getType() {
        return type;
    }

    @Override
    public DorisConstant getExpectedValue() {
        DorisConstant expectedValue = getExpression().getExpectedValue();
        if (expectedValue == null) {
            return null;
        }
        return expectedValue.cast(type);
    }

    @Override
    public DorisDataType getExpectedType() {
        return type;
    }
}
