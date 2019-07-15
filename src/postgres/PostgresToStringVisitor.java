package postgres;

import java.util.List;
import java.util.stream.Collectors;

import lama.Randomly;
import lama.postgres.ast.PostgresComputableFunction;
import postgres.PostgresSchema.PostgresColumn;
import postgres.PostgresSchema.PostgresDataType;
import postgres.ast.PostgresBetweenOperation;
import postgres.ast.PostgresBinaryArithmeticOperation;
import postgres.ast.PostgresBinaryComparisonOperation;
import postgres.ast.PostgresBinaryLogicalOperation;
import postgres.ast.PostgresCastOperation;
import postgres.ast.PostgresColumnValue;
import postgres.ast.PostgresConcatOperation;
import postgres.ast.PostgresConstant;
import postgres.ast.PostgresExpression;
import postgres.ast.PostgresInOperation;
import postgres.ast.PostgresLikeOperation;
import postgres.ast.PostgresOrderByTerm;
import postgres.ast.PostgresPostfixOperation;
import postgres.ast.PostgresPrefixOperation;
import postgres.ast.PostgresSelect;

public class PostgresToStringVisitor extends PostgresVisitor {

	private final StringBuilder sb = new StringBuilder();

	public void visit(PostgresConstant constant) {
		sb.append(constant.getTextRepresentation());
	}

	public String get() {
		return sb.toString();
	}

	@Override
	public void visit(PostgresPostfixOperation op) {
		sb.append("(");
		visit(op.getExpression());
		sb.append(")");
		sb.append(" ");
		sb.append(op.getOperatorTextRepresentation());
	}

	@Override
	public void visit(PostgresColumnValue c) {
		sb.append(c.getColumn().getFullQualifiedName());
	}

	@Override
	public void visit(PostgresPrefixOperation op) {
		sb.append(op.getTextRepresentation());
		sb.append(" (");
		visit(op.getExpression());
		sb.append(")");
	}

	@Override
	public void visit(PostgresBinaryLogicalOperation op) {
		sb.append("(");
		visit(op.getLeft());
		sb.append(") ");
		sb.append(op.getOp());
		sb.append(" (");
		visit(op.getRight());
		sb.append(")");
	}

	@Override
	public void visit(PostgresSelect s) {
		sb.append("SELECT ");
		switch (s.getFromOptions()) {
		case DISTINCT:
			sb.append("DISTINCT ");
			break;
		case ALL:
			sb.append(Randomly.fromOptions("ALL ", ""));
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
				PostgresColumn column = s.getFetchColumns().get(i);
				sb.append(column.getTable().getName());
				sb.append('.');
				sb.append(column.getName());
				// Postgres does not allow duplicate column names
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

		if (s.getWhereClause() != null) {
			PostgresExpression whereClause = s.getWhereClause();
			sb.append(" WHERE ");
			visit(whereClause);
		}
		if (s.getGroupByClause() != null && s.getGroupByClause().size() > 0) {
			sb.append(" ");
			sb.append("GROUP BY ");
			List<PostgresExpression> groupBys = s.getGroupByClause();
			for (int i = 0; i < groupBys.size(); i++) {
				if (i != 0) {
					sb.append(", ");
				}
				visit(groupBys.get(i));
			}
		}
		if (!s.getOrderByClause().isEmpty()) {
			sb.append(" ORDER BY ");
			List<PostgresExpression> orderBys = s.getOrderByClause();
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
	public void visit(PostgresOrderByTerm op) {
		visit(op.getExpr());
		sb.append(" ");
		sb.append(op.getOrder());
	}

	@Override
	public void visit(PostgresBinaryComparisonOperation op) {
		sb.append("(");
		visit(op.getLeft());
		sb.append(") ");
		sb.append(op.getOp().getTextRepresentation());
		sb.append(" (");
		visit(op.getRight());
		sb.append(")");
		if (op.getLeft().getExpressionType() == PostgresDataType.TEXT
				&& op.getRight().getExpressionType() == PostgresDataType.TEXT) {
			sb.append(" COLLATE \"C\"");
		}
	}

	@Override
	public void visit(PostgresComputableFunction f) {
		sb.append(f.getFunction().getName());
		sb.append("(");
		int i = 0;
		for (PostgresExpression arg : f.getArguments()) {
			if (i++ != 0) {
				sb.append(", ");
			}
			visit(arg);
		}
		sb.append(")");
	}

	@Override
	public void visit(PostgresCastOperation cast) {
		if (Randomly.getBoolean()) {
			sb.append("CAST(");
			visit(cast.getExpression());
			sb.append(" AS ");
			appendType(cast);
			sb.append(")");
		} else {
			sb.append("(");
			visit(cast.getExpression());
			sb.append(")::");
			appendType(cast);
		}
	}

	private void appendType(PostgresCastOperation cast) {
		switch (cast.getType()) {
		case BOOLEAN:
			sb.append("BOOLEAN");
			break;
		case INT: // TODO support also other int types
			sb.append("BIGINT");
			break;
		case TEXT:
			sb.append("TEXT");
			break;
		default:
			throw new AssertionError(cast.getType());
		}
	}

	@Override
	public void visit(PostgresLikeOperation op) {
		visit(op.getLeft());
		sb.append(" LIKE ");
		visit(op.getRight());
	}

	@Override
	public void visit(PostgresBinaryArithmeticOperation op) {
		sb.append("(");
		visit(op.getLeft());
		sb.append(") ");
		sb.append(op.getOp().getTextRepresentation());
		sb.append(" (");
		visit(op.getRight());
		sb.append(")");
	}

	@Override
	public void visit(PostgresBetweenOperation op) {
		sb.append("(");
		visit(op.getExpr());
		if ((op.getExpr().getExpressionType() == PostgresDataType.TEXT
				&& op.getLeft().getExpressionType() == PostgresDataType.TEXT)) {
			sb.append(" COLLATE \"C\"");
		}
		sb.append(") BETWEEN ");
		if (op.isSymmetric()) {
			sb.append("SYMMETRIC ");
		}
		sb.append("(");
		visit(op.getLeft());
		sb.append(") AND (");
		visit(op.getRight());
		if (op.getExpr().getExpressionType() == PostgresDataType.TEXT
				&& op.getRight().getExpressionType() == PostgresDataType.TEXT) {
			sb.append(" COLLATE \"C\"");
		}
		sb.append(")");
	}

	@Override
	public void visit(PostgresConcatOperation op) {
		sb.append("((");
		visit(op.getLeft());
		sb.append(") || (");
		visit(op.getRight());
		sb.append("))");
	}

	@Override
	public void visit(PostgresInOperation op) {
		sb.append("(");
		visit(op.getExpr());
		sb.append(")");
		if (!op.isTrue()) {
			sb.append(" NOT");
		}
		sb.append(" IN (");
		for (int i = 0; i < op.getListElements().size(); i++) {
			if (i != 0) {
				sb.append(", ");
			}
			visit(op.getListElements().get(i));
		}
		sb.append(")");
	}

}
