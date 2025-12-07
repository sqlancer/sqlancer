package sqlancer.spark.ast;

import java.util.List;

import sqlancer.common.ast.newast.NewInOperatorNode;

public class SparkInOperation extends NewInOperatorNode<SparkExpression> implements SparkExpression {

    public SparkInOperation(SparkExpression left, List<SparkExpression> right, boolean isNegated) {
        super(left, right, isNegated);
    }
}