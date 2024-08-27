package sqlancer.datafusion.ast;

import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.newast.NewUnaryPrefixOperatorNode;

public class DataFusionUnaryPrefixOperation extends NewUnaryPrefixOperatorNode<DataFusionExpression>
        implements DataFusionExpression {
    public DataFusionUnaryPrefixOperation(DataFusionExpression expr, BinaryOperatorNode.Operator operator) {
        super(expr, operator);
    }
}
