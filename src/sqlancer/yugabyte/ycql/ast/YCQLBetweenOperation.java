package sqlancer.yugabyte.ycql.ast;

import sqlancer.common.ast.newast.NewBetweenOperatorNode;

public class YCQLBetweenOperation extends NewBetweenOperatorNode<YCQLExpression> implements YCQLExpression {
    public YCQLBetweenOperation(YCQLExpression left, YCQLExpression middle, YCQLExpression right, boolean isTrue) {
        super(left, middle, right, isTrue);
    }
}
