package sqlancer.spark.ast;

import java.util.List;

import sqlancer.common.ast.newast.NewFunctionNode;

public class SparkFunction<F> extends NewFunctionNode<SparkExpression, F> implements SparkExpression {

    public SparkFunction(List<SparkExpression> args, F func) {
        super(args, func);
    }

}
