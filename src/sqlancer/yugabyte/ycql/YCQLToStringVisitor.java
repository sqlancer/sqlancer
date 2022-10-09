package sqlancer.yugabyte.ycql;

import sqlancer.common.ast.newast.NewToStringVisitor;
import sqlancer.common.ast.newast.Node;
import sqlancer.yugabyte.ycql.ast.YCQLConstant;
import sqlancer.yugabyte.ycql.ast.YCQLExpression;
import sqlancer.yugabyte.ycql.ast.YCQLSelect;

public class YCQLToStringVisitor extends NewToStringVisitor<YCQLExpression> {

    @Override
    public void visitSpecific(Node<YCQLExpression> expr) {
        if (expr instanceof YCQLConstant) {
            visit((YCQLConstant) expr);
        } else if (expr instanceof YCQLSelect) {
            visit((YCQLSelect) expr);
        } else {
            throw new AssertionError(expr.getClass());
        }
    }

    private void visit(YCQLConstant constant) {
        sb.append(constant.toString());
    }

    private void visit(YCQLSelect select) {
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

    public static String asString(Node<YCQLExpression> expr) {
        YCQLToStringVisitor visitor = new YCQLToStringVisitor();
        visitor.visit(expr);
        return visitor.get();
    }

}
