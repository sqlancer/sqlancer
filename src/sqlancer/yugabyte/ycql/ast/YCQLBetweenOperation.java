package sqlancer.yugabyte.ycql.ast;

import sqlancer.common.ast.newast.NewBetweenOperatorNode;

public class YCQLBetweenOperator extends NewBetweenOperatorNode<YCQLExpression> implements YCQLExpression {
    public YCQLBetweenOperator(YCQLExpression left, YCQLExpression middle, YCQLExpression right,
            boolean isTrue) {
        super(left, middle, right, isTrue);
    }
}
