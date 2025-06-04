package sqlancer.oxla.ast;

import sqlancer.common.ast.newast.TableReferenceNode;
import sqlancer.oxla.OxlaToStringVisitor;
import sqlancer.oxla.schema.OxlaTable;

public class OxlaTableReference extends TableReferenceNode<OxlaExpression, OxlaTable>
        implements OxlaExpression {
    public OxlaTableReference(OxlaTable table) {
        super(table);
    }

    @Override
    public String toString() {
        return OxlaToStringVisitor.asString(this);
    }
}
