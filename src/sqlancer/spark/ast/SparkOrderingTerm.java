package sqlancer.spark.ast;

import sqlancer.common.ast.newast.NewOrderingTerm;

public class SparkOrderingTerm extends NewOrderingTerm<SparkExpression> implements SparkExpression {

    public SparkOrderingTerm(SparkExpression expr, Ordering ordering) {
        super(expr, ordering);
    }
}
