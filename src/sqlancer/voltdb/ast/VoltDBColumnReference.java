package sqlancer.voltdb.ast;

import sqlancer.common.ast.newast.ColumnReferenceNode;
import sqlancer.voltdb.VoltDBSchema.VoltDBColumn;

public class VoltDBColumnReference extends ColumnReferenceNode<VoltDBExpression, VoltDBColumn>
        implements VoltDBExpression {

    public VoltDBColumnReference(VoltDBColumn column) {
        super(column);
    }

}
