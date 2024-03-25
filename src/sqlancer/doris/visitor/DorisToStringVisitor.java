package sqlancer.doris.visitor;

import sqlancer.Randomly;
import sqlancer.common.ast.newast.NewToStringVisitor;
import sqlancer.common.ast.newast.Node;
import sqlancer.doris.ast.DorisCaseOperation;
import sqlancer.doris.ast.DorisCastOperation;
import sqlancer.doris.ast.DorisConstant;
import sqlancer.doris.ast.DorisExpression;
import sqlancer.doris.ast.DorisFunctionOperation;
import sqlancer.doris.ast.DorisJoin;
import sqlancer.doris.ast.DorisSelect;

public class DorisToStringVisitor extends NewToStringVisitor<DorisExpression> {

    @Override
    public void visitSpecific(Node<DorisExpression> expr) {
        if (expr instanceof DorisConstant) {
            visit((DorisConstant) expr);
        } else if (expr instanceof DorisSelect) {
            visit((DorisSelect) expr);
        } else if (expr instanceof DorisJoin) {
            visit((DorisJoin) expr);
        } else if (expr instanceof DorisCastOperation) {
            visit((DorisCastOperation) expr);
        } else if (expr instanceof DorisCaseOperation) {
            visit((DorisCaseOperation) expr);
        } else if (expr instanceof DorisFunctionOperation) {
            visit((DorisFunctionOperation) expr);
        } else {
            throw new AssertionError(expr.getClass());
        }
    }

    private void visit(DorisJoin join) {
        sb.append(" ");
        visit(join.getLeftTable());
        sb.append(" ");
        switch (join.getJoinType()) {
        case INNER:
            if (Randomly.getBoolean()) {
                sb.append("INNER ");
            } else {
                sb.append("CROSS ");
            }
            sb.append("JOIN ");
            break;
        case LEFT:
            sb.append("LEFT ");
            if (Randomly.getBoolean()) {
                sb.append(" OUTER ");
            }
            sb.append("JOIN ");
            break;
        case RIGHT:
            sb.append("RIGHT ");
            if (Randomly.getBoolean()) {
                sb.append(" OUTER ");
            }
            sb.append("JOIN ");
            break;
        case STRAIGHT:
            sb.append("STRAIGHT_JOIN ");
            break;
        default:
            throw new AssertionError();
        }
        visit(join.getRightTable());
        sb.append(" ");
        if (join.getOnCondition() != null) {
            sb.append("ON ");
            visit(join.getOnCondition());
        }
    }

    private void visit(DorisConstant constant) {
        sb.append(constant.toString());
    }

    private void visit(DorisCastOperation castExpr) {
        sb.append("CAST(");
        visit(castExpr.getExpr());
        sb.append(" AS ");
        sb.append(castExpr.getType().toString());
        sb.append(") ");
    }

    private void visit(DorisFunctionOperation func) {
        sb.append(func.getFunction().getFunctionName());
        sb.append("(");

        if (func.getArgs() != null) {
            for (int i = 0; i < func.getArgs().size(); i++) {
                visit(DorisExprToNode.cast(func.getArgs().get(i)));
                if (i != func.getArgs().size() - 1) {
                    sb.append(",");
                }
            }
        }
        sb.append(") ");
    }

    private void visit(DorisCaseOperation cases) {
        sb.append("CASE ");
        visit(DorisExprToNode.cast(cases.getExpr()));
        sb.append(" ");
        for (int i = 0; i < cases.getConditions().size(); i++) {
            DorisExpression predicate = cases.getConditions().get(i);
            DorisExpression then = cases.getThenClauses().get(i);
            sb.append(" WHEN ");
            visit(DorisExprToNode.cast(predicate));
            sb.append(" THEN ");
            visit(DorisExprToNode.cast(then));
            sb.append(" ");
        }
        if (cases.getElseClause() != null) {
            sb.append("ELSE ");
            visit(DorisExprToNode.cast(cases.getElseClause()));
            sb.append(" ");
        }
        sb.append("END ");
    }

    private void visit(DorisSelect select) {
        sb.append("SELECT ");
        if (select.isDistinct()) {
            sb.append("DISTINCT ");
        }
        visit(select.getFetchColumns());
        sb.append(" FROM ");
        visit(select.getFromList());
        if (!select.getFromList().isEmpty() && !select.getJoinList().isEmpty()) {
            sb.append(", ");
        }
        if (!select.getJoinList().isEmpty()) {
            visit(select.getJoinList());
        }
        if (select.getWhereClause() != null) {
            sb.append(" WHERE ");
            visit(select.getWhereClause());
        }
        if (!select.getGroupByExpressions().isEmpty()) {
            sb.append(" GROUP BY ");
            visit(select.getGroupByExpressions());
        }
        if (select.getHavingClause() != null) {
            sb.append(" HAVING ");
            visit(select.getHavingClause());
        }
        if (!select.getOrderByClauses().isEmpty()) {
            sb.append(" ORDER BY ");
            visit(select.getOrderByClauses());
        }
        if (select.getLimitClause() != null) {
            sb.append(" LIMIT ");
            visit(select.getLimitClause());
        }
        if (select.getOffsetClause() != null) {
            sb.append(" OFFSET ");
            visit(select.getOffsetClause());
        }
    }

    public static String asString(Node<DorisExpression> expr) {
        DorisToStringVisitor visitor = new DorisToStringVisitor();
        visitor.visit(expr);
        return visitor.get();
    }

    public static String asString(DorisExpression expr) {
        DorisToStringVisitor visitor = new DorisToStringVisitor();
        visitor.visit(DorisExprToNode.cast(expr));
        return visitor.get();
    }
}
