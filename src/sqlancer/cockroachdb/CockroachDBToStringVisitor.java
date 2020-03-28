package sqlancer.cockroachdb;

.Collectors;

import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.cockroachdb.ast.CockroachDBAggregate;
import sqlancer.cockroachdb.ast.CockroachDBBetweenOperation;
import sqlancer.cockroachdb.ast.CockroachDBCaseOperation;
import sqlancer.cockroachdb.ast.CockroachDBColumnReference;
import sqlancer.cockroachdb.ast.CockroachDBConstant;
import sqlancer.cockroachdb.ast.CockroachDBExpression;
import sqlancer.cockroachdb.ast.CockroachDBFunctionCall;
import sqlancer.cockroachdb.ast.CockroachDBInOperation;
import sqlancer.cockroachdb.ast.CockroachDBJoin;
import sqlancer.cockroachdb.ast.CockroachDBMultiValuedComparison;
import sqlancer.cockroachdb.ast.CockroachDBSelect;
import sqlancer.cockroachdb.ast.CockroachDBTableReference;
import sqlancer.visitor.ToStringVisitor;

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
		visit(call.getArguments());
		sb.append(")");
	}

	@Override
	public void visit(CockroachDBInOperation inOp) {
		sb.append("(");
		visit(inOp.getLeft());
		sb.append(") IN (");
		visit(inOp.getRight());
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
		visit(select.getColumns());
		sb.append(" FROM ");
		if (!select.getFromTables().isEmpty()) {
			visit(select.getFromTables().stream().map(t -> (CockroachDBExpression) t).collect(Collectors.toList()));
		}
		if (!select.getFromTables().isEmpty() && !select.getJoinList().isEmpty()) {
			sb.append(", ");
		}
		visit(select.getJoinList().stream().map(j -> (CockroachDBExpression) j).collect(Collectors.toList()));
		if (select.getWhereCondition() != null) {
			sb.append(" WHERE ");
			visit(select.getWhereCondition());
		}
		if (select.getGroupByExpression() != null && !select.getGroupByExpression().isEmpty()) {
			sb.append(" GROUP BY ");
			visit(select.getGroupByExpression());
		}
		if (select.getHavingClause() != null) {
			sb.append(" HAVING ");
			visit(select.getHavingClause());
		}
		if (!select.getOrderByTerms().isEmpty()) {
			sb.append(" ORDER BY ");
			visit(select.getOrderByTerms());
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

	@Override
	public void visit(CockroachDBJoin join) {
		visit(join.getLeftTable());
		switch (join.getJoinType()) {
		case INNER:
			sb.append(" INNER ");
			potentiallyAddHint();
			sb.append("JOIN ");
			visit(join.getRightTable());
			sb.append(" ON ");
			visit(join.getOnCondition());
			break;
		case NATURAL:
			sb.append(" NATURAL ");
//			potentiallyAddHint();
			sb.append("JOIN ");
			visit(join.getRightTable());
			break;
		case CROSS:
			sb.append(" CROSS ");
			potentiallyAddHint();
			sb.append("JOIN ");
			visit(join.getRightTable());
			break;
		case OUTER:
			sb.append(" ");
			switch (join.getOuterType()) {
			case FULL:
				sb.append("FULL");
				break;
			case LEFT:
				sb.append("LEFT");
				break;
			case RIGHT:
				sb.append("RIGHT");
				break;
			default:
				throw new AssertionError();
			}
			sb.append(" OUTER ");
			potentiallyAddHint();
			sb.append("JOIN ");
			visit(join.getRightTable());
			sb.append(" ON ");
			visit(join.getOnCondition());
			break;
		default:
			throw new AssertionError();
		}
	}

	private void potentiallyAddHint() {
		if (Randomly.getBoolean()) {
			sb.append(Randomly.fromOptions("HASH", "MERGE", "LOOKUP"));
			sb.append(" ");
		}
	}

	@Override
	public void visit(CockroachDBTableReference tableRef) {
		sb.append(tableRef.getTable().getName());
	}

	@Override
	public void visit(CockroachDBAggregate aggr) {
		sb.append(aggr.getFunc().name());
		sb.append("(");
		visit(aggr.getExpr());
		sb.append(")");
	}

	@Override
	public void visit(CockroachDBMultiValuedComparison comp) {
		sb.append("(");
		visit(comp.getLeft());
		sb.append(" ");
		sb.append(comp.getOp().getStringRepr());
		sb.append(" ");
		sb.append(comp.getType());
		sb.append(" (");
		visit(comp.getRight());
		sb.append(")");
		sb.append(")");
	}
}
