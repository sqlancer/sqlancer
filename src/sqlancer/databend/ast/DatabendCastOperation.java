package sqlancer.databend.ast;

import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.newast.NewUnaryPostfixOperatorNode;
import sqlancer.common.ast.newast.Node;
import sqlancer.databend.DatabendSchema;

public class DatabendCastOperation extends NewUnaryPostfixOperatorNode<DatabendExpression> {

    public DatabendCastOperation(Node<DatabendExpression> expr, DatabendSchema.DatabendCompositeDataType type) {
        super(expr, new BinaryOperatorNode.Operator() {
            @Override
            public String getTextRepresentation() {
                return "::" + type.toString();
            }
        });
    }

}
