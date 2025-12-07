package sqlancer.spark.ast;

import sqlancer.common.ast.newast.ColumnReferenceNode;
import sqlancer.spark.SparkSchema.SparkColumn;

public class SparkColumnReference extends ColumnReferenceNode<SparkExpression, SparkColumn> implements SparkExpression {

    public SparkColumnReference(SparkColumn column) {
        super(column);
    }
}