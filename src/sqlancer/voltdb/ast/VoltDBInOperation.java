package sqlancer.voltdb.ast;

import java.util.List;

import sqlancer.common.ast.newast.NewInOperatorNode;

public class VoltDBInOperation extends NewInOperatorNode<VoltDBExpression> implements VoltDBExpression {

    public VoltDBInOperation(VoltDBExpression left, List<VoltDBExpression> right, boolean isNegated) {
        super(left, right, isNegated);
    }

}
