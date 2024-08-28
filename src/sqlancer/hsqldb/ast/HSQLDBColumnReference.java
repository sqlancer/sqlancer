package sqlancer.hsqldb.ast;

import sqlancer.common.ast.newast.ColumnReferenceNode;
import sqlancer.hsqldb.HSQLDBSchema;

public class HSQLDBColumnReference extends ColumnReferenceNode<HSQLDBExpression, HSQLDBSchema.HSQLDBColumn>
        implements HSQLDBExpression {

    public HSQLDBColumnReference(HSQLDBSchema.HSQLDBColumn column) {
        super(column);
    }
}
