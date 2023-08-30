package sqlancer.presto;

import sqlancer.common.ast.newast.NewToStringVisitor;
import sqlancer.common.ast.newast.Node;
import sqlancer.presto.ast.PrestoAtTimeZoneOperator;
import sqlancer.presto.ast.PrestoCastFunction;
import sqlancer.presto.ast.PrestoConstant;
import sqlancer.presto.ast.PrestoExpression;
import sqlancer.presto.ast.PrestoFunctionWithoutParenthesis;
import sqlancer.presto.ast.PrestoJoin;
import sqlancer.presto.ast.PrestoMultiValuedComparison;
import sqlancer.presto.ast.PrestoQuantifiedComparison;
import sqlancer.presto.ast.PrestoSelect;

public class PrestoToStringVisitor extends NewToStringVisitor<PrestoExpression> {

    public static String asString(Node<PrestoExpression> expr) {
        PrestoToStringVisitor visitor = new PrestoToStringVisitor();
        visitor.visit(expr);
        return visitor.get();
    }

    @Override
    public void visitSpecific(Node<PrestoExpression> expr) {
        if (expr instanceof PrestoConstant) {
            visit((PrestoConstant) expr);
        } else if (expr instanceof PrestoSelect) {
            visit((PrestoSelect) expr);
        } else if (expr instanceof PrestoJoin) {
            visit((PrestoJoin) expr);
        } else if (expr instanceof PrestoCastFunction) {
            visit((PrestoCastFunction) expr);
        } else if (expr instanceof PrestoFunctionWithoutParenthesis) {
            visit((PrestoFunctionWithoutParenthesis) expr);
        } else if (expr instanceof PrestoAtTimeZoneOperator) {
            visit((PrestoAtTimeZoneOperator) expr);
        } else if (expr instanceof PrestoMultiValuedComparison) {
            visit((PrestoMultiValuedComparison) expr);
        } else if (expr instanceof PrestoQuantifiedComparison) {
            visit((PrestoQuantifiedComparison) expr);
        } else {
            throw new AssertionError(expr.getClass());
        }
    }

    private void visit(PrestoJoin join) {
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

    private void visit(PrestoConstant constant) {
        sb.append(constant.toString());
    }

    private void visit(PrestoAtTimeZoneOperator timeZoneOperator) {
        visit(timeZoneOperator.getExpr());
        sb.append(" AT TIME ZONE ");
        sb.append(timeZoneOperator.getTimeZone());
    }

    private void visit(PrestoFunctionWithoutParenthesis prestoFunctionWithoutParenthesis) {
        sb.append(prestoFunctionWithoutParenthesis.getExpr());
    }

    private void visit(PrestoSelect select) {
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
        if (!select.getOrderByExpressions().isEmpty()) {
            sb.append(" ORDER BY ");
            visit(select.getOrderByExpressions());
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

    public void visit(PrestoCastFunction cast) {
        sb.append("CAST((");
        visit(cast.getExpr());
        sb.append(") AS ");
        sb.append(cast.getType().toString());
        sb.append(")");
    }

    public void visit(PrestoMultiValuedComparison comp) {
        sb.append("(");
        visit(comp.getLeft());
        sb.append(" ");
        sb.append(comp.getOp().getStringRepresentation());
        sb.append(" ");
        sb.append(comp.getType());
        sb.append(" (VALUES ");
        visit(comp.getRight());
        sb.append(")");
        sb.append(")");
    }

    public void visit(PrestoQuantifiedComparison comp) {
        sb.append("(");
        visit(comp.getLeft());
        sb.append(" ");
        sb.append(comp.getOp().getStringRepresentation());
        sb.append(" ");
        sb.append(comp.getType());
        sb.append(" ( ");
        visit(comp.getRight());
        sb.append(" ) ");
        sb.append(")");
    }
}
