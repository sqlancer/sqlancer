package sqlancer.datafusion;

import java.util.List;

import sqlancer.common.ast.newast.NewToStringVisitor;
import sqlancer.common.ast.newast.Node;
import sqlancer.datafusion.ast.DataFusionConstant;
import sqlancer.datafusion.ast.DataFusionExpression;
import sqlancer.datafusion.ast.DataFusionJoin;
import sqlancer.datafusion.ast.DataFusionSelect;

public class DataFusionToStringVisitor extends NewToStringVisitor<DataFusionExpression> {

    public static String asString(Node<DataFusionExpression> expr) {
        DataFusionToStringVisitor visitor = new DataFusionToStringVisitor();
        visitor.visit(expr);
        return visitor.get();
    }

    public static String asString(List<Node<DataFusionExpression>> exprs) {
        DataFusionToStringVisitor visitor = new DataFusionToStringVisitor();
        visitor.visit(exprs);
        return visitor.get();
    }

    @Override
    public void visitSpecific(Node<DataFusionExpression> expr) {
        if (expr instanceof DataFusionConstant) {
            visit((DataFusionConstant) expr);
        } else if (expr instanceof DataFusionSelect) {
            visit((DataFusionSelect) expr);
        } else if (expr instanceof DataFusionJoin) {
            visit((DataFusionJoin) expr);
        } else {
            throw new AssertionError(expr.getClass());
        }
    }

    private void visit(DataFusionJoin join) {
        visit(join.getLeftTable());
        sb.append(" ");
        sb.append(join.getJoinType());
        sb.append(" ");

        sb.append(" JOIN ");
        visit(join.getRightTable());
        if (join.getOnCondition() != null) {
            sb.append(" ON ");
            visit(join.getOnCondition());
        }
    }

    private void visit(DataFusionConstant constant) {
        sb.append(constant.toString());
    }

    private void visit(DataFusionSelect select) {
        sb.append("SELECT ");
        if (select.fetchColumnsString.isPresent()) {
            sb.append(select.fetchColumnsString.get());
        } else {
            visit(select.getFetchColumns());
        }

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

}
