package sqlancer.hsqldb.ast;

import sqlancer.common.ast.newast.ColumnReferenceNode;
import sqlancer.hsqldb.HSQLDBSchema;

public class HSQLDBColumnReference extends ColumnReferenceNode<HSQLDBExpression, HSQLDBSchema.HSQLDBColumn> {

    public HSQLDBColumnReference(HSQLDBSchema.HSQLDBColumn column) {
        super(column);
    }
}
