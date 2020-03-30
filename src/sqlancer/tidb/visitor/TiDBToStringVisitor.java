package sqlancer.tidb.visitor;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.tidb.ast.TiDBColumnReference;
import sqlancer.tidb.ast.TiDBConstant;
import sqlancer.tidb.ast.TiDBExpression;
import sqlancer.tidb.ast.TiDBFunctionCall;
import sqlancer.tidb.ast.TiDBJoin;
import sqlancer.tidb.ast.TiDBJoin.JoinType;
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
		visit(select.getFetchColumns());
		sb.append(" FROM ");
		visit(select.getFromList());
		if (!select.getFromList().isEmpty() && !select.getJoinExpressions().isEmpty()) {
			sb.append(", ");
		}
		if (!select.getJoinExpressions().isEmpty()) {
			visit(select.getJoinExpressions());
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
	}

	@Override
	public void visit(TiDBFunctionCall call) {
		sb.append(call.getFunction());
		sb.append("(");
		visit(call.getArgs());
		sb.append(")");
	}

	@Override
	public void visit(TiDBJoin join) {
		sb.append(" ");
		visit(join.getLeftTable());
		sb.append(" ");
		switch (join.getJoinType()) {
		case INNER:
			if (Randomly.getBoolean()) {
				sb.append("INNER ");
			} else {
				sb.append("CROSS ");
			}
			sb.append("JOIN ");
			break;
		case LEFT:
			sb.append("LEFT ");
			if (Randomly.getBoolean()) {
				sb.append(" OUTER ");
			}
			sb.append("JOIN ");
			if (true) {
				// https://github.com/pingcap/tidb/issues/15846
				throw new IgnoreMeException();
			}
			break;
		case RIGHT:
			sb.append("RIGHT ");
			if (Randomly.getBoolean()) {
				sb.append(" OUTER ");
			}
			sb.append("JOIN ");
			break;
		case STRAIGHT:
			sb.append("STRAIGHT_JOIN ");
			break;
		case NATURAL:
			sb.append("NATURAL ");
			switch (join.getNaturalJoinType()) {
			case INNER:
				break;
			case LEFT:
				sb.append("LEFT ");
				if (true) {
					// https://github.com/pingcap/tidb/issues/15846
					throw new IgnoreMeException();
				}
				break;
			case RIGHT:
				if (true) {
					// https://github.com/pingcap/tidb/issues/15844
					throw new IgnoreMeException();
				}
				sb.append("RIGHT ");
				break;
			}
			sb.append("JOIN ");
			break;
		}
		visit(join.getRightTable());
		sb.append(" ");
		if (join.getJoinType() != JoinType.NATURAL) {
			sb.append("ON ");
			visit(join.getOnCondition());
		}
	}
}