package sqlancer.spark.ast;

import java.util.List;

import sqlancer.common.ast.newast.NewCaseOperatorNode;

public class SparkCaseOperation extends NewCaseOperatorNode<SparkExpression> implements SparkExpression {

    public SparkCaseOperation(SparkExpression switchCondition, List<SparkExpression> conditions,
            List<SparkExpression> expressions, SparkExpression elseExpr) {
        super(switchCondition, conditions, expressions, elseExpr);
    }
}
