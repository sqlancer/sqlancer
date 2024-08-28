package sqlancer.datafusion.ast;

import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.newast.NewUnaryPostfixOperatorNode;

public class DataFusionUnaryPostfixOperation extends NewUnaryPostfixOperatorNode<DataFusionExpression>
        implements DataFusionExpression {
    public DataFusionUnaryPostfixOperation(DataFusionExpression expr, BinaryOperatorNode.Operator op) {
        super(expr, op);
    }
}
