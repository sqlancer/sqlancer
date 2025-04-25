package sqlancer.oxla;

import sqlancer.common.ast.newast.NewToStringVisitor;
import sqlancer.oxla.ast.OxlaConstant;
import sqlancer.oxla.ast.OxlaExpression;
import sqlancer.oxla.ast.OxlaJoin;
import sqlancer.oxla.ast.OxlaSelect;

public class OxlaToStringVisitor extends NewToStringVisitor<OxlaExpression> {
    private static final OxlaToStringVisitor visitor = new OxlaToStringVisitor();

    public static synchronized String asString(OxlaExpression expr) {
        visitor.reset();
        visitor.visit(expr);
        return visitor.get();
    }

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
        } else {
            throw new AssertionError(expr.getClass());
        }
    }

    private void visit(OxlaConstant constant) {
        sb.append(constant.toString());
    }

    private void visit(OxlaSelect select) {
        sb.append("SELECT ");
        if (select.type == OxlaSelect.SelectType.DISTINCT) {
            sb.append("DISTINCT ");
        }
        if (select.getFetchColumns() != null) {
            visit(select.getFetchColumns());
        } else {
            sb.append('*');
        }

        sb.append(" FROM ");
        visit(select.getFromList());

        if (select.getJoinList() != null) {
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

    private void visit(OxlaJoin join) {
        sb.append(String.format(" %s JOIN ", join.type));
        if (join.onClause != null) {
            sb.append(" ON ");
            visit(join.onClause);
        }
    }
}
