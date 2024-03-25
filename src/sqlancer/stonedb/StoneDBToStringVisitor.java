package sqlancer.stonedb;

import sqlancer.common.ast.newast.NewToStringVisitor;
import sqlancer.common.ast.newast.Node;
import sqlancer.stonedb.StoneDBSchema.StoneDBDataType;
import sqlancer.stonedb.ast.StoneDBConstant;
import sqlancer.stonedb.ast.StoneDBExpression;
import sqlancer.stonedb.ast.StoneDBJoin;
import sqlancer.stonedb.ast.StoneDBSelect;
import sqlancer.stonedb.gen.StoneDBExpressionGenerator.StoneDBCastOperation;

public class StoneDBToStringVisitor extends NewToStringVisitor<StoneDBExpression> {
    @Override
    public void visitSpecific(Node<StoneDBExpression> expr) {
        if (expr instanceof StoneDBConstant) {
            visit((StoneDBConstant) expr);
        } else if (expr instanceof StoneDBSelect) {
            visit((StoneDBSelect) expr);
        } else if (expr instanceof StoneDBJoin) {
            visit((StoneDBJoin) expr);
        } else if (expr instanceof StoneDBCastOperation) {
            visit((StoneDBCastOperation) expr);
        } else {
            throw new AssertionError(expr.getClass());
        }
    }

    private void visit(StoneDBConstant constant) {
        sb.append(constant.toString());
    }

    private void visit(StoneDBSelect select) {
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

    private void visit(StoneDBJoin join) {
        visit(join.getLeftTable());
        sb.append(" ");
        sb.append(join.getJoinType());
        sb.append(" ");
        if (join.getNaturalJoinType() != null) {
            sb.append(join.getNaturalJoinType());
        }
        sb.append(" JOIN ");
        visit(join.getRightTable());
        if (join.getOnCondition() != null) {
            sb.append(" ON ");
            visit(join.getOnCondition());
        }
    }

    private void visit(StoneDBCastOperation cast) {
        sb.append("CAST(");
        visit(cast.getExpr());
        sb.append(" AS ");
        sb.append(cast.getType() == StoneDBDataType.INT ? "UNSIGNED" : cast.getType().toString());
        sb.append(") ");
    }

    public static String asString(Node<StoneDBExpression> expr) {
        StoneDBToStringVisitor visitor = new StoneDBToStringVisitor();
        visitor.visit(expr);
        return visitor.get();
    }
}
