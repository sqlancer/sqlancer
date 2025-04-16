package sqlancer.hive.ast;

import sqlancer.common.ast.newast.NewBetweenOperatorNode;

public class HiveBetweenOperation extends NewBetweenOperatorNode<HiveExpression>
        implements HiveExpression {
    
    public HiveBetweenOperation(HiveExpression left, HiveExpression middle, HiveExpression right,
            boolean isTrue) {
        super(left, middle, right, isTrue);
    }
}
