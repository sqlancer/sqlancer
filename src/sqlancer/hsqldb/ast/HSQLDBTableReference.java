package sqlancer.hsqldb.ast;

import sqlancer.common.ast.newast.TableReferenceNode;
import sqlancer.hsqldb.HSQLDBSchema;

public class HSQLDBTableReference extends TableReferenceNode<HSQLDBExpression, HSQLDBSchema.HSQLDBTable>
        implements HSQLDBExpression {
    public HSQLDBTableReference(HSQLDBSchema.HSQLDBTable table) {
        super(table);
    }
}
