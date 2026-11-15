package sqlancer.voltdb.ast;

import sqlancer.common.ast.newast.TableReferenceNode;
import sqlancer.voltdb.VoltDBSchema.VoltDBTable;

public class VoltDBTableReference extends TableReferenceNode<VoltDBExpression, VoltDBTable>
        implements VoltDBExpression {

    public VoltDBTableReference(VoltDBTable table) {
        super(table);
    }

}
