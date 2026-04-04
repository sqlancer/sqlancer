package sqlancer.spark.ast;

import sqlancer.common.ast.newast.NewBetweenOperatorNode;

public class SparkBetweenOperation extends NewBetweenOperatorNode<SparkExpression> implements SparkExpression {

    public SparkBetweenOperation(SparkExpression left, SparkExpression middle, SparkExpression right, boolean isTrue) {
        super(left, middle, right, isTrue);
    }
}
