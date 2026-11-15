package sqlancer.voltdb.ast;

import sqlancer.common.ast.newast.NewBetweenOperatorNode;

public class VoltDBBetweenOperation extends NewBetweenOperatorNode<VoltDBExpression> implements VoltDBExpression {

    public VoltDBBetweenOperation(VoltDBExpression left, VoltDBExpression middle, VoltDBExpression right,
            boolean isTrue) {
        super(left, middle, right, isTrue);
    }

}
