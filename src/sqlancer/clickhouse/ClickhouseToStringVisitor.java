package sqlancer.clickhouse;

import sqlancer.ast.ClickhouseSelect;
import sqlancer.ast.newast.NewToStringVisitor;
import sqlancer.ast.newast.Node;
import sqlancer.clickhouse.ast.ClickhouseConstant;
import sqlancer.clickhouse.ast.ClickhouseExpression;

public class ClickhouseToStringVisitor extends NewToStringVisitor<ClickhouseExpression> {

	@Override
	public void visitSpecific(Node<ClickhouseExpression> expr) {
		if (expr instanceof ClickhouseConstant) {
			visit((ClickhouseConstant) expr);
		} else if (expr instanceof ClickhouseSelect) {
			visit((ClickhouseSelect) expr);
		} else {
			throw new AssertionError(expr.getClass());
		}
	}

	private void visit(ClickhouseConstant constant) {
		sb.append(constant.toString());
	}

	private void visit(ClickhouseSelect select) {
		sb.append("SELECT ");
		visit(select.getFetchColumns());
		sb.append(" FROM ");
		visit(select.getFromList());
//		if (!select.getFromList().isEmpty() /* && !select.getJoinExpressions().isEmpty() */) {
//			sb.append(", ");
//		}
//		if (!select.getJoinExpressions().isEmpty()) {
//			visit(select.getJoinExpressions());
//		}
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
	}

	public static String asString(Node<ClickhouseExpression> expr) {
		ClickhouseToStringVisitor visitor = new ClickhouseToStringVisitor();
		visitor.visit(expr);
		return visitor.get();
	}

}
