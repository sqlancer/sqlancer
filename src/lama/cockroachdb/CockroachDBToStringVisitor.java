package lama.cockroachdb;


import java.util.List;
import java.util.stream.Collectors;

import lama.Randomly;
import lama.cockroachdb.ast.CockroachDBBetweenOperation;
import lama.cockroachdb.ast.CockroachDBCaseOperation;
import lama.cockroachdb.ast.CockroachDBColumnReference;
import lama.cockroachdb.ast.CockroachDBConstant;
import lama.cockroachdb.ast.CockroachDBExpression;
import lama.cockroachdb.ast.CockroachDBFunctionCall;
import lama.cockroachdb.ast.CockroachDBInOperation;
import lama.cockroachdb.ast.CockroachDBSelect;
import lama.visitor.ToStringVisitor;

public class CockroachDBToStringVisitor extends ToStringVisitor<CockroachDBExpression> implements CockroachDBVisitor {
	
	@Override
	public void visitSpecific(CockroachDBExpression expr) {
		CockroachDBVisitor.super.visit(expr);
	}
	
	@Override
	public void visit(CockroachDBConstant c) {
		sb.append(c.toString());
	}
	
	public String getString() {
		return sb.toString();
	}

	@Override
	public void visit(CockroachDBColumnReference c) {
		if (c.getColumn().getTable() == null) {
			sb.append(c.getColumn().getName());
		} else {
			sb.append(c.getColumn().getFullQualifiedName());
		}
	}


	@Override
	public void visit(CockroachDBFunctionCall call) {
		sb.append(call.getName());
		sb.append("(");
		visitList(call.getArguments());
		sb.append(")");
	}

	private void visitList(List<CockroachDBExpression> list) {
		for (int i = 0; i < list.size(); i++) {
			if (i != 0) {
				sb.append(", ");
			}
			visit(list.get(i));
		}
	}

	@Override
	public void visit(CockroachDBInOperation inOp) {
		sb.append("(");
		visit(inOp.getLeft());
		sb.append(") IN (");
		visitList(inOp.getRight());
		sb.append(")");
	}

	@Override
	public void visit(CockroachDBBetweenOperation op) {
		sb.append("(");
		visit(op.getExpr());
		sb.append(")");
		sb.append(" ");
		sb.append(op.getType().getStringRepresentation());
		sb.append(" (");
		visit(op.getLeft());
		sb.append(") AND (");
		visit(op.getRight());
		sb.append(")");
	}

	@Override
	public void visit(CockroachDBSelect select) {
		sb.append("SELECT ");
		if (select.isDistinct()) {
			sb.append("DISTINCT ");
		} else if (Randomly.getBoolean()) {
			sb.append("ALL ");
		}
		visitList(select.getColumns());
		sb.append(" FROM ");
		sb.append(select.getFromTables().stream().map(t -> t.getName()).collect(Collectors.joining(", ")));
		if (select.getWhereCondition() != null) {
			sb.append(" WHERE ");
			visit(select.getWhereCondition());
		}
		if (select.getGroupByExpression() != null) {
			sb.append(" GROUP BY ");
			visitList(select.getGroupByExpression());
		}
		if (select.getHavingClause() != null) {
			sb.append(" HAVING ");
			visit(select.getHavingClause());
		}
		if (!select.getOrderByTerms().isEmpty()) {
			sb.append(" ORDER BY ");
			visitList(select.getOrderByTerms());
		}
		if (select.getLimit() != null) {
			sb.append(" LIMIT ");
			visit(select.getLimit());
		}
		if (select.getOffset() != null) {
			sb.append(" OFFSET ");
			visit(select.getOffset());
		}
	}

	@Override
	public void visit(CockroachDBCaseOperation cases) {
		sb.append("CASE ");
		for (int i = 0; i < cases.getConditions().size(); i++) {
			CockroachDBExpression predicate = cases.getConditions().get(i);
			CockroachDBExpression then = cases.getThenClauses().get(i);
			sb.append(" WHEN ");
			visit(predicate);
			sb.append(" THEN ");
			visit(then);
			sb.append(" ");
		}
		if (cases.getElseClause() != null) {
			sb.append("ELSE ");
			visit(cases.getElseClause());
			sb.append(" ");
		}
		sb.append("END");
	}
}
