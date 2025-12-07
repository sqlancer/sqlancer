package sqlancer.spark.ast;

import sqlancer.common.ast.newast.Expression;
import sqlancer.spark.SparkSchema.SparkColumn;

public interface SparkExpression extends Expression<SparkColumn> {
}