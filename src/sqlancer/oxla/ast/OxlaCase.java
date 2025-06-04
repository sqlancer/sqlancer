package sqlancer.oxla.ast;

import sqlancer.common.ast.newast.NewCaseOperatorNode;
import sqlancer.oxla.OxlaToStringVisitor;

import java.util.List;

public class OxlaCase extends NewCaseOperatorNode<OxlaExpression>
        implements OxlaExpression {
    public OxlaCase(OxlaExpression switchCondition,
                    List<OxlaExpression> conditions,
                    List<OxlaExpression> expressions,
                    OxlaExpression elseExpr) {
        super(switchCondition, conditions, expressions, elseExpr);
    }

    @Override
    public String toString() {
        return OxlaToStringVisitor.asString(this);
    }
}
