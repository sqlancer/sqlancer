package sqlancer.oxla.ast;

import sqlancer.common.ast.newast.ColumnReferenceNode;
import sqlancer.oxla.schema.OxlaColumn;

public class OxlaColumnReference extends ColumnReferenceNode<OxlaExpression, OxlaColumn>
        implements OxlaExpression {
    public OxlaColumnReference(OxlaColumn oxlaColumn) {
        super(oxlaColumn);
    }
}
