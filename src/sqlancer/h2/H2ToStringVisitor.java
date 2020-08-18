package sqlancer.h2;

import sqlancer.common.ast.newast.NewToStringVisitor;
import sqlancer.common.ast.newast.Node;

public class H2ToStringVisitor extends NewToStringVisitor<H2Expression> {

    @Override
    public void visitSpecific(Node<H2Expression> expr) {
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
        visit(join.getLeftTable());
        sb.append(" ");
        sb.append(join.getJoinType());
        sb.append(" JOIN ");
        visit(join.getRightTable());
        if (join.getOnCondition() != null) {
            sb.append(" ON ");
            visit(join.getOnCondition());
        }
    }

    public void visit(H2Select select) {
        sb.append("SELECT ");
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

    public static String asString(Node<H2Expression> expr) {
        H2ToStringVisitor visitor = new H2ToStringVisitor();
        visitor.visit(expr);
        return visitor.get();
    }

}
