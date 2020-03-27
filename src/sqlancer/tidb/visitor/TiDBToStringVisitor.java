package sqlancer.tidb.visitor;

import java.util.List;

import sqlancer.tidb.ast.TiDBColumnReference;
import sqlancer.tidb.ast.TiDBConstant;
import sqlancer.tidb.ast.TiDBExpression;
import sqlancer.tidb.ast.TiDBFunctionCall;
import sqlancer.tidb.ast.TiDBSelect;
import sqlancer.tidb.ast.TiDBTableReference;
import sqlancer.visitor.ToStringVisitor;

public class TiDBToStringVisitor extends ToStringVisitor<TiDBExpression> implements TiDBVisitor {

	@Override
	public void visitSpecific(TiDBExpression expr) {
		TiDBVisitor.super.visit(expr);
	}

	@Override
	public void visit(TiDBConstant c) {
		sb.append(c.toString());
	}

	public String getString() {
		return sb.toString();
	}

	@Override
	public void visit(TiDBColumnReference c) {
		if (c.getColumn().getTable() == null) {
			sb.append(c.getColumn().getName());
		} else {
			sb.append(c.getColumn().getFullQualifiedName());
		}
	}

	@Override
	public void visit(TiDBTableReference expr) {
		sb.append(expr.getTable().getName());
	}

	@Override
	public void visit(TiDBSelect select) {
		sb.append("SELECT ");
		visitList(select.getFetchColumns());
		sb.append(" FROM ");
		visitList(select.getFrom());
		if (select.getWherePredicate() != null) {
			sb.append(" WHERE ");
			visit(select.getWherePredicate());
		}
		if (!select.getOrderBy().isEmpty()) {
			sb.append(" ORDER BY ");
			visitList(select.getOrderBy());
		}
	}

	private void visitList(List<TiDBExpression> expressions) {
		for (int i = 0; i < expressions.size(); i++) {
			if (i != 0) {
				sb.append(", ");
			}
			visit(expressions.get(i));
		}
	}

	@Override
	public void visit(TiDBFunctionCall call) {
		sb.append(call.getFunction());
		sb.append("(");
		visitList(call.getArgs());
		sb.append(")");
	}
}