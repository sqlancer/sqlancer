package sqlancer.presto.ast;

import sqlancer.common.ast.newast.ColumnReferenceNode;
import sqlancer.presto.PrestoSchema;

public class PrestoColumnReference extends ColumnReferenceNode<PrestoExpression, PrestoSchema.PrestoColumn>
        implements PrestoExpression {

    public PrestoColumnReference(PrestoSchema.PrestoColumn column) {
        super(column);
    }

}
