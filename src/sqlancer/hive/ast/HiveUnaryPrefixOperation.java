package sqlancer.hive.ast;

import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.common.ast.newast.NewUnaryPrefixOperatorNode;

public class HiveUnaryPrefixOperation extends NewUnaryPrefixOperatorNode<HiveExpression> 
        implements HiveExpression {

    public HiveUnaryPrefixOperation(HiveExpression expr, Operator op) {
        super(expr, op);
    }
    
}
