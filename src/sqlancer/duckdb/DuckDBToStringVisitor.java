package sqlancer.duckdb;

import sqlancer.common.ast.newast.NewToStringVisitor;
import sqlancer.common.ast.newast.Node;
import sqlancer.duckdb.ast.DuckDBConstant;
import sqlancer.duckdb.ast.DuckDBExpression;
import sqlancer.duckdb.ast.DuckDBJoin;
import sqlancer.duckdb.ast.DuckDBSelect;

public class DuckDBToStringVisitor extends NewToStringVisitor<DuckDBExpression> {

    @Override
    public void visitSpecific(Node<DuckDBExpression> expr) {
        if (expr instanceof DuckDBConstant) {
            visit((DuckDBConstant) expr);
        } else if (expr instanceof DuckDBSelect) {
            visit((DuckDBSelect) expr);
        } else if (expr instanceof DuckDBJoin) {
            visit((DuckDBJoin) expr);
        } else {
            throw new AssertionError(expr.getClass());
        }
    }

    private void visit(DuckDBJoin join) {
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

    private void visit(DuckDBConstant constant) {
        sb.append(constant.toString());
    }

    private void visit(DuckDBSelect select) {
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

    public static String asString(Node<DuckDBExpression> expr) {
        DuckDBToStringVisitor visitor = new DuckDBToStringVisitor();
        visitor.visit(expr);
        return visitor.get();
    }

}
