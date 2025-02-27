package sqlancer.databend;

import sqlancer.common.ast.newast.NewToStringVisitor;
import sqlancer.databend.ast.DatabendConstant;
import sqlancer.databend.ast.DatabendExpression;
import sqlancer.databend.ast.DatabendJoin;
import sqlancer.databend.ast.DatabendSelect;

public class DatabendToStringVisitor extends NewToStringVisitor<DatabendExpression> {

    @Override
    public void visitSpecific(DatabendExpression expr) {
        if (expr instanceof DatabendConstant) {
            visit((DatabendConstant) expr);
        } else if (expr instanceof DatabendSelect) {
            visit((DatabendSelect) expr);
        } else if (expr instanceof DatabendJoin) {
            visit((DatabendJoin) expr);
        } else {
            throw new AssertionError(expr.getClass());
        }
    }

    private void visit(DatabendJoin join) {
        visit(join.getLeftTable());
        sb.append(" ");
        sb.append(join.getJoinType());
        sb.append(" ");
        if (join.getOuterType() != null) {
            sb.append(join.getOuterType());
        }
        sb.append(" JOIN ");
        visit(join.getRightTable());
        if (join.getOnCondition() != null) {
            sb.append(" ON ");
            visit(join.getOnCondition());
        }
    }

    private void visit(DatabendConstant constant) {
        sb.append(constant.toString());
    }

    // private void visitFromList(List<Node<DatabendExpression>> fromList) {
    // for (int i = 0; i < fromList.size(); i++) {
    // if (i != 0) {
    // sb.append(" INNER JOIN ");
    // }
    // visit(fromList.get(i));
    // }
    // }

    private void visit(DatabendSelect select) {
        visitSelect(select);
    }

    public static String asString(DatabendExpression expr) {
        DatabendToStringVisitor visitor = new DatabendToStringVisitor();
        visitor.visit(expr);
        return visitor.get();
    }

}
