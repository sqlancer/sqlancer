package sqlancer.hsqldb;

import sqlancer.common.ast.newast.NewToStringVisitor;
import sqlancer.common.ast.newast.Node;
import sqlancer.hsqldb.ast.HSQLDBConstant;
import sqlancer.hsqldb.ast.HSQLDBExpression;
import sqlancer.hsqldb.ast.HSQLDBJoin;
import sqlancer.hsqldb.ast.HSQLDBSelect;

public class HSQLDBToStringVisitor extends NewToStringVisitor<HSQLDBExpression> {

    @Override
    public void visitSpecific(Node<HSQLDBExpression> expr) {
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

    public static String asString(Node<HSQLDBExpression> expr) {
        HSQLDBToStringVisitor visitor = new HSQLDBToStringVisitor();
        visitor.visit(expr);
        return visitor.get();
    }

    private void visit(HSQLDBJoin join) {
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

    private void visit(HSQLDBConstant constant) {
        sb.append(constant.toString());
    }

    private void visit(HSQLDBSelect select) {
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
}
