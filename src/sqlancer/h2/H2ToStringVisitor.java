package sqlancer.h2;

import sqlancer.common.ast.newast.NewToStringVisitor;
import sqlancer.h2.ast.H2CastNode;
import sqlancer.h2.ast.H2Constant;
import sqlancer.h2.ast.H2Expression;
import sqlancer.h2.ast.H2Join;
import sqlancer.h2.ast.H2Select;

public class H2ToStringVisitor extends NewToStringVisitor<H2Expression> {

    @Override
    public void visitSpecific(H2Expression expr) {
        if (expr instanceof H2Constant) {
            visit((H2Constant) expr);
        } else if (expr instanceof H2Select) {
            visit((H2Select) expr);
        } else if (expr instanceof H2Join) {
            visit((H2Join) expr);
        } else if (expr instanceof H2CastNode) {
            visit((H2CastNode) expr);
        } else {
            throw new AssertionError(expr.getClass());
        }
    }

    private void visit(H2Constant constant) {
        sb.append(constant.toString());
    }

    private void visit(H2CastNode cast) {
        sb.append("CAST(");
        visit(cast.getExpression());
        sb.append(" AS ");
        sb.append(cast.getType());
        sb.append(')');
    }

    private void visit(H2Join join) {
        visit((H2Expression) join.getLeftTable());
        sb.append(" ");
        sb.append(join.getJoinType());
        sb.append(" JOIN ");
        visit((H2Expression) join.getRightTable());
        if (join.getOnCondition() != null) {
            sb.append(" ON ");
            visit(join.getOnCondition());
        }
    }

    public void visit(H2Select select) {
        visitSelect(select);
    }

    public static String asString(H2Expression expr) {
        H2ToStringVisitor visitor = new H2ToStringVisitor();
        visitor.visit(expr);
        return visitor.get();
    }

}
