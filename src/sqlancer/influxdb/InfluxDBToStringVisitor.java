package sqlancer.influxdb;

import sqlancer.common.ast.newast.NewToStringVisitor;
import sqlancer.common.ast.newast.TableReferenceNode;
import sqlancer.influxdb.ast.InfluxDBConstant;
import sqlancer.influxdb.ast.InfluxDBExpression;
import sqlancer.influxdb.ast.InfluxDBJoin;
import sqlancer.influxdb.ast.InfluxDBSelect;

public class InfluxDBToStringVisitor extends NewToStringVisitor<InfluxDBExpression> {

    @Override
    public void visitSpecific(InfluxDBExpression expr) {
        if (expr instanceof InfluxDBConstant) {
            visit((InfluxDBConstant) expr);
        } else if (expr instanceof InfluxDBSelect) {
            visit((InfluxDBSelect) expr);
        } else if (expr instanceof InfluxDBJoin) {
            visit((InfluxDBJoin) expr);
        } else {
            throw new AssertionError(expr.getClass());
        }
    }

    private void visit(InfluxDBJoin join) {
        visit((TableReferenceNode<InfluxDBExpression, InfluxDBSchema.InfluxDBTable>) join.getLeftTable());
        sb.append(" ");
        sb.append(join.getJoinType());
        sb.append(" ");
        if (join.getOuterType() != null) {
            sb.append(join.getOuterType());
        }
        sb.append(" JOIN ");
        visit((TableReferenceNode<InfluxDBExpression, InfluxDBSchema.InfluxDBTable>) join.getRightTable());
        if (join.getOnCondition() != null) {
            sb.append(" ON ");
            visit(join.getOnCondition());
        }
    }

    private void visit(InfluxDBConstant constant) {
        sb.append(constant.toString());
    }

    private void visit(InfluxDBSelect select) {
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

    public static String asString(InfluxDBExpression expr) {
        InfluxDBToStringVisitor visitor = new InfluxDBToStringVisitor();
        visitor.visit(expr);
        return visitor.get();
    }
}