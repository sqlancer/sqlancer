package sqlancer.oxla;

import sqlancer.common.ast.newast.NewToStringVisitor;
import sqlancer.oxla.ast.OxlaConstant;
import sqlancer.oxla.ast.OxlaExpression;
import sqlancer.oxla.ast.OxlaJoin;
import sqlancer.oxla.ast.OxlaSelect;

public class OxlaToStringVisitor extends NewToStringVisitor<OxlaExpression> {
    public void reset() {
        // Java's STL is stupid; to reset a StringBuilder they recommended to allocate a new one - in performance
        // critical scenarios such as this we get stupid performance hits - reset the string's length to 0 instead.
        sb.setLength(0);
    }

    @Override
    public void visitSpecific(OxlaExpression expr) {
        if (expr instanceof OxlaConstant) {
            visit((OxlaConstant) expr);
        } else if (expr instanceof OxlaSelect) {
            visit((OxlaSelect) expr);
        } else if (expr instanceof OxlaJoin) {
            visit((OxlaJoin) expr);
        }

        throw new AssertionError(expr.getClass());
    }

    private void visit(OxlaConstant constant) {
        sb.append(constant.toString());
    }

    private void visit(OxlaSelect select) {
    }

    private void visit(OxlaJoin join) {

    }
}
