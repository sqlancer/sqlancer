package sqlancer.hive.ast;

import java.util.List;

import sqlancer.common.ast.newast.NewInOperatorNode;

public class HiveInOperation extends NewInOperatorNode<HiveExpression> implements HiveExpression {

    public HiveInOperation(HiveExpression left, List<HiveExpression> right, boolean isNegated) {
        super(left, right, isNegated);
    }
}
