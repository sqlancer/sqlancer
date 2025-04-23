package sqlancer.hive;

import sqlancer.common.ast.newast.NewToStringVisitor;
import sqlancer.common.ast.newast.TableReferenceNode;
import sqlancer.hive.ast.HiveConstant;
import sqlancer.hive.ast.HiveExpression;
import sqlancer.hive.ast.HiveJoin;
import sqlancer.hive.ast.HiveSelect;
import sqlancer.hive.ast.HiveCastOperation;

public class HiveToStringVisitor extends NewToStringVisitor<HiveExpression> {

    @Override
    public void visitSpecific(HiveExpression expr) {
        if (expr instanceof HiveConstant) {
            visit((HiveConstant) expr);
        } else if (expr instanceof HiveSelect) {
            visit((HiveSelect) expr);
        } else if (expr instanceof HiveJoin) {
            visit((HiveJoin) expr);
        } else if (expr instanceof HiveCastOperation) {
            visit((HiveCastOperation) expr);
        } else {
            throw new AssertionError(expr.getClass());
        }
    }

    private void visit(HiveConstant constant) {
        sb.append(constant.toString());
    }

    private void visit(HiveSelect select) {
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

    private void visit(HiveJoin join) {
        switch (join.getJoinType()) {
        case INNER:
            sb.append(" INNER JOIN ");
            break;
        case LEFT_OUTER:
            sb.append(" LEFT JOIN ");
            break;
        case RIGHT_OUTER:
            sb.append(" RIGHT JOIN ");
            break;
        case FULL_OUTER:
            sb.append(" FULL JOIN ");
            break;
        case LEFT_SEMI:
            sb.append(" LEFT SEMI JOIN ");
            break;
        case CROSS:
            sb.append(" CROSS JOIN ");
            break;
        default:
            throw new UnsupportedOperationException();
        }
        visit((TableReferenceNode<HiveExpression, HiveSchema.HiveTable>) join.getRightTable());
        if (join.getOnClause() != null) {
            sb.append(" ON ");
            visit(join.getOnClause());
        }
    }

    private void visit(HiveCastOperation cast) {
        sb.append("CAST(");
        visit(cast.getExpression());
        sb.append(" AS ");
        sb.append(cast.getType());
        sb.append(")");
    }

    public static String asString(HiveExpression expr) {
        HiveToStringVisitor visitor = new HiveToStringVisitor();
        visitor.visit(expr);
        return visitor.get();
    }
}
