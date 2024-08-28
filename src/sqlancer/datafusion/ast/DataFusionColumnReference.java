package sqlancer.datafusion.ast;

import sqlancer.common.ast.newast.ColumnReferenceNode;
import sqlancer.datafusion.DataFusionSchema;

public class DataFusionColumnReference extends
        ColumnReferenceNode<DataFusionExpression, DataFusionSchema.DataFusionColumn> implements DataFusionExpression {
    public DataFusionColumnReference(DataFusionSchema.DataFusionColumn column) {
        super(column);
    }

}
