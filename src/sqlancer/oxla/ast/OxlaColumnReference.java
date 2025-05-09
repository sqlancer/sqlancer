package sqlancer.oxla.ast;

import sqlancer.common.ast.newast.ColumnReferenceNode;
import sqlancer.oxla.schema.OxlaColumn;

public class OxlaColumnReference extends ColumnReferenceNode<OxlaExpression, OxlaColumn>
        implements OxlaExpression {
    private final OxlaConstant expectedValue;

    public OxlaColumnReference(OxlaColumn oxlaColumn, OxlaConstant expectedValue) {
        super(oxlaColumn);
        this.expectedValue = expectedValue;
    }
}
