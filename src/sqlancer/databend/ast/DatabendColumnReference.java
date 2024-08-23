package sqlancer.databend.ast;

import sqlancer.common.ast.newast.ColumnReferenceNode;
import sqlancer.databend.DatabendSchema;

public class DatabendColumnReference extends ColumnReferenceNode<DatabendExpression, DatabendSchema.DatabendColumn>
        implements DatabendExpression {
    public DatabendColumnReference(DatabendSchema.DatabendColumn column) {
        super(column);
    }
}
