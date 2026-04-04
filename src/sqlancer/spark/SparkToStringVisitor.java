package sqlancer.spark;

import sqlancer.common.ast.newast.NewToStringVisitor;
import sqlancer.common.ast.newast.TableReferenceNode;
import sqlancer.spark.ast.SparkCastOperation;
import sqlancer.spark.ast.SparkConstant;
import sqlancer.spark.ast.SparkExpression;
import sqlancer.spark.ast.SparkJoin;
import sqlancer.spark.ast.SparkSelect;

public class SparkToStringVisitor extends NewToStringVisitor<SparkExpression> {

    @Override
    public void visitSpecific(SparkExpression expr) {
        if (expr instanceof SparkConstant) {
            visit((SparkConstant) expr);
        } else if (expr instanceof SparkSelect) {
            visit((SparkSelect) expr);
        } else if (expr instanceof SparkJoin) {
            visit((SparkJoin) expr);
        } else if (expr instanceof SparkCastOperation) {
            visit((SparkCastOperation) expr);
        } else {
            throw new AssertionError(expr.getClass());
        }
    }

    private void visit(SparkConstant constant) {
        sb.append(constant.toString());
    }

    private void visit(SparkSelect select) {
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
        // Spark supports OFFSET, though strictly usually with LIMIT or in newer
        // versions
        if (select.getOffsetClause() != null) {
            sb.append(" OFFSET ");
            visit(select.getOffsetClause());
        }
    }

    private void visit(SparkJoin join) {
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
        // Spark also supports LEFT ANTI, which Hive might lack in some older versions
        case LEFT_ANTI:
            sb.append(" LEFT ANTI JOIN ");
            break;
        case CROSS:
            sb.append(" CROSS JOIN ");
            break;
        default:
            throw new UnsupportedOperationException("Join type not supported in Spark visitor: " + join.getJoinType());
        }
        visit((TableReferenceNode<SparkExpression, SparkSchema.SparkTable>) join.getRightTable());
        if (join.getOnClause() != null) {
            sb.append(" ON ");
            visit(join.getOnClause());
        }
    }

    private void visit(SparkCastOperation cast) {
        sb.append("CAST(");
        visit(cast.getExpression());
        sb.append(" AS ");
        sb.append(cast.getType());
        sb.append(")");
    }

    public static String asString(SparkExpression expr) {
        SparkToStringVisitor visitor = new SparkToStringVisitor();
        visitor.visit(expr);
        return visitor.get();
    }
}
