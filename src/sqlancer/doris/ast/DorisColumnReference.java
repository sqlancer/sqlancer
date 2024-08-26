package sqlancer.doris.ast;

import sqlancer.common.ast.newast.ColumnReferenceNode;
import sqlancer.doris.DorisSchema;

public class DorisColumnReference extends ColumnReferenceNode<DorisExpression, DorisSchema.DorisColumn>
        implements DorisExpression {
    public DorisColumnReference(DorisSchema.DorisColumn column) {
        super(column);
    }
}
