package sqlancer.datafusion.ast;

import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.common.ast.newast.NewBinaryOperatorNode;

public class DataFusionBinaryOperation extends NewBinaryOperatorNode<DataFusionExpression>
        implements DataFusionExpression {
    public DataFusionBinaryOperation(DataFusionExpression left, DataFusionExpression right, Operator op) {
        super(left, right, op);
    }
}
