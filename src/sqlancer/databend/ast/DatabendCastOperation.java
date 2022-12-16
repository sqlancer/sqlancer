package sqlancer.databend.ast;

import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.newast.NewUnaryPostfixOperatorNode;
import sqlancer.common.ast.newast.Node;
import sqlancer.databend.DatabendSchema.DatabendCompositeDataType;
import sqlancer.databend.DatabendSchema.DatabendDataType;

public class DatabendCastOperation extends NewUnaryPostfixOperatorNode<DatabendExpression>
        implements DatabendExpression {

    DatabendDataType type;

    public DatabendCastOperation(Node<DatabendExpression> expr, DatabendCompositeDataType type) {
        super(expr, new BinaryOperatorNode.Operator() {
            @Override
            public String getTextRepresentation() {
                return "::" + type.toString();
            }
        });
        this.type = type.getPrimitiveDataType();
    }

    DatabendExpression getExpression() {
        return (DatabendExpression) getExpr();
    }

    @Override
    public DatabendConstant getExpectedValue() {
        DatabendConstant expectedValue = getExpression().getExpectedValue();
        if (expectedValue == null) {
            return null;
        }
        return expectedValue.cast(type);
    }

    @Override
    public DatabendDataType getExpectedType() {
        return type;
    }
}
