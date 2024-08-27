package sqlancer.presto.ast;

import sqlancer.common.ast.newast.NewBetweenOperatorNode;

public class PrestoBetweenOperation extends NewBetweenOperatorNode<PrestoExpression> implements PrestoExpression {
    public PrestoBetweenOperation(PrestoExpression left, PrestoExpression middle, PrestoExpression right,
            boolean isTrue) {
        super(left, middle, right, isTrue);
    }
}
