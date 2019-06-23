package lama.mysql;

import java.util.List;
import java.util.stream.Collectors;

import lama.Randomly;
import lama.mysql.MySQLSchema.MySQLColumn;
import lama.mysql.ast.MySQLBinaryComparisonOperation;
import lama.mysql.ast.MySQLBinaryLogicalOperation;
import lama.mysql.ast.MySQLCastOperation;
import lama.mysql.ast.MySQLColumnValue;
import lama.mysql.ast.MySQLComputableFunction;
import lama.mysql.ast.MySQLConstant;
import lama.mysql.ast.MySQLExpression;
import lama.mysql.ast.MySQLInOperation;
import lama.mysql.ast.MySQLJoin;
import lama.mysql.ast.MySQLSelect;
import lama.mysql.ast.MySQLUnaryPrefixOperation;
import lama.mysql.ast.MySQLUnaryPostfixOperator;

public class MySQLToStringVisitor extends MySQLVisitor {

	StringBuffer sb = new StringBuffer();

	@Override
	public void visit(MySQLSelect s) {
		sb.append("SELECT ");
		switch (s.getFromOptions()) {
		case DISTINCT:
			sb.append("DISTINCT ");
			break;
		case ALL:
			sb.append(Randomly.fromOptions("ALL ", ""));
			break;
		case DISTINCTROW:
			sb.append("DISTINCTROW ");
			break;
		default:
			throw new AssertionError();
		}
		sb.append(s.getModifiers().stream().collect(Collectors.joining(" ")));
		if (s.getModifiers().size() > 0) {
			sb.append(" ");
		}
		if (s.getFetchColumns() == null) {
			sb.append("*");
		} else {
			for (int i = 0; i < s.getFetchColumns().size(); i++) {
				if (i != 0) {
					sb.append(", ");
				}
				MySQLColumn column = s.getFetchColumns().get(i);
				sb.append(column.getTable().getName());
				sb.append('.');
				sb.append(column.getName());
				// MySQL does not allow duplicate column names
				sb.append(" AS ");
				sb.append(column.getTable().getName());
				sb.append(column.getName());
			}
		}
		sb.append(" FROM ");
		for (int i = 0; i < s.getFromList().size(); i++) {
			if (i != 0) {
				sb.append(", ");
			}
			sb.append(s.getFromList().get(i).getName());
		}
		for (MySQLJoin j : s.getJoinClauses()) {
			visit(j);
		}

		if (s.getWhereClause() != null) {
			MySQLExpression whereClause = s.getWhereClause();
			sb.append(" WHERE ");
			visit(whereClause);
		}
		if (s.getGroupByClause() != null && s.getGroupByClause().size() > 0) {
			sb.append(" ");
			sb.append("GROUP BY ");
			List<MySQLExpression> groupBys = s.getGroupByClause();
			for (int i = 0; i < groupBys.size(); i++) {
				if (i != 0) {
					sb.append(", ");
				}
				visit(groupBys.get(i));
			}
		}
		if (!s.getOrderByClause().isEmpty()) {
			sb.append(" ORDER BY ");
			List<MySQLExpression> orderBys = s.getOrderByClause();
			for (int i = 0; i < orderBys.size(); i++) {
				if (i != 0) {
					sb.append(", ");
				}
				visit(s.getOrderByClause().get(i));
			}
		}
		if (s.getLimitClause() != null) {
			sb.append(" LIMIT ");
			visit(s.getLimitClause());
		}

		if (s.getOffsetClause() != null) {
			sb.append(" OFFSET ");
			visit(s.getOffsetClause());
		}
	}

	@Override
	public void visit(MySQLConstant constant) {
		sb.append(constant.getTextRepresentation());
	}

	public String get() {
		return sb.toString();
	}

	@Override
	public void visit(MySQLColumnValue column) {
		sb.append(column.getColumn().getFullQualifiedName());
	}

	@Override
	public void visit(MySQLUnaryPrefixOperation column) {
		sb.append(column.getOperatorTextRepresentation());
		sb.append("(");
		visit(column.getExpression());
		sb.append(")");
	}

	@Override
	public void visit(MySQLUnaryPostfixOperator op) {
		sb.append("(");
		visit(op.getExpression());
		sb.append(")");
		sb.append(" IS ");
		if (op.isNegated()) {
			sb.append("NOT ");
		}
		switch (op.getOperator()) {
		case IS_FALSE:
			sb.append("FALSE");
			break;
		case IS_NULL:
			if (Randomly.getBoolean()) {
				sb.append("UNKNOWN");
			} else {
				sb.append("NULL");
			}
			break;
		case IS_TRUE:
			sb.append("TRUE");
			break;
		default:
			throw new AssertionError(op);
		}
	}
	
	@Override
	public void visit(MySQLComputableFunction f) {
		sb.append(f.getFunction().getName());
		sb.append("(");
		for (int i = 0; i < f.getArguments().length; i++) {
			if (i != 0) {
				sb.append(", ");
			}
			visit(f.getArguments()[i]);
		}
		sb.append(")");
	}

	@Override
	public void visit(MySQLBinaryLogicalOperation op) {
		sb.append("(");
		visit(op.getLeft());
		sb.append(")");
		sb.append(" ");
		sb.append(op.getTextRepresentation());
		sb.append(" ");
		sb.append("(");
		visit(op.getRight());
		sb.append(")");
	}

	@Override
	public void visit(MySQLBinaryComparisonOperation op) {
		sb.append("(");
		visit(op.getLeft());
		sb.append(") ");
		sb.append(op.getOp().getTextRepresentation());
		sb.append(" (");
		visit(op.getRight());
		sb.append(")");
	}

	@Override
	public void visit(MySQLCastOperation op) {
		sb.append("CAST(");
		visit(op.getExpr());
		sb.append(" AS ");
		sb.append(op.getType());
		sb.append(")");
	}

	@Override
	public void visit(MySQLInOperation op) {
		sb.append("(");
		visit(op.getExpr());
		sb.append(")");
		sb.append(" IN ");
		sb.append("(");
		for (int i = 0; i < op.getListElements().size(); i++) {
			if (i != 0) {
				sb.append(", ");
			}
			visit(op.getListElements().get(i));
		}
		sb.append(")");
	}
	
}
