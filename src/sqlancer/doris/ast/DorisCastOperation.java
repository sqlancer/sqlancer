package sqlancer.doris.ast;

import sqlancer.common.ast.newast.Node;
import sqlancer.doris.DorisSchema.DorisCompositeDataType;
import sqlancer.doris.DorisSchema.DorisDataType;

public class DorisCastOperation implements Node<DorisExpression>, DorisExpression {
    Node<DorisExpression> expr;
    DorisDataType type;

    public DorisCastOperation(Node<DorisExpression> expr, DorisCompositeDataType type) {
        this.expr = expr;
        this.type = type.getPrimitiveDataType();
    }

    public DorisCastOperation(Node<DorisExpression> expr, DorisDataType type) {
        this.expr = expr;
        this.type = type;
    }

    public Node<DorisExpression> getExpr() {
        return expr;
    }

    public DorisExpression getExpression() {
        return (DorisExpression) expr;
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
