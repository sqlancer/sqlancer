package sqlancer.spark.ast;

import sqlancer.common.ast.newast.TableReferenceNode;
import sqlancer.spark.SparkSchema;

public class SparkTableReference extends TableReferenceNode<SparkExpression, SparkSchema.SparkTable>
        implements SparkExpression {

    public SparkTableReference(SparkSchema.SparkTable table) {
        super(table);
    }

}