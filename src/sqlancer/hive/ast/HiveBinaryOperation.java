package sqlancer.hive.ast;

import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.common.ast.newast.NewBinaryOperatorNode;

public class HiveBinaryOperation extends NewBinaryOperatorNode<HiveExpression> 
        implements HiveExpression {
    
    public HiveBinaryOperation(HiveExpression left, HiveExpression right, Operator op) {
        super(left, right, op);
    }
}
