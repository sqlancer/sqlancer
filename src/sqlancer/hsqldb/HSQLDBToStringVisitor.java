package sqlancer.hsqldb;

import sqlancer.common.ast.newast.NewToStringVisitor;
import sqlancer.hsqldb.ast.HSQLDBConstant;
import sqlancer.hsqldb.ast.HSQLDBExpression;
import sqlancer.hsqldb.ast.HSQLDBJoin;
import sqlancer.hsqldb.ast.HSQLDBSelect;

public class HSQLDBToStringVisitor extends NewToStringVisitor<HSQLDBExpression> {

    @Override
    public void visitSpecific(HSQLDBExpression expr) {
        if (expr instanceof HSQLDBConstant) {
            visit((HSQLDBConstant) expr);
        } else if (expr instanceof HSQLDBSelect) {
            visit((HSQLDBSelect) expr);
        } else if (expr instanceof HSQLDBJoin) {
            visit((HSQLDBJoin) expr);
        } else {
            throw new AssertionError(expr.getClass());
        }
    }

    public static String asString(HSQLDBExpression expr) {
        HSQLDBToStringVisitor visitor = new HSQLDBToStringVisitor();
        visitor.visit(expr);
        return visitor.get();
    }

    private void visit(HSQLDBJoin join) {
        visit((HSQLDBExpression) join.getLeftTable());
        sb.append(" ");
        sb.append(join.getJoinType());
        sb.append(" ");
        if (join.getOuterType() != null) {
            sb.append(join.getOuterType());
        }
        sb.append(" JOIN ");
        visit((HSQLDBExpression) join.getRightTable());
        if (join.getOnCondition() != null) {
            sb.append(" ON ");
            visit(join.getOnCondition());
        }
    }

    private void visit(HSQLDBConstant constant) {
        sb.append(constant.toString());
    }

    private void visit(HSQLDBSelect select) {
        visitSelect(select);
    }
}
