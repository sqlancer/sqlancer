package sqlancer.hive.ast;

import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.common.ast.newast.NewUnaryPostfixOperatorNode;

public class HiveUnaryPostfixOperation extends NewUnaryPostfixOperatorNode<HiveExpression> implements HiveExpression {

    public HiveUnaryPostfixOperation(HiveExpression expr, Operator op) {
        super(expr, op);
    }

}
