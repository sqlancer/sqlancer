package lama.postgres;

import java.util.List;
import java.util.Optional;

import lama.Randomly;
import lama.postgres.PostgresSchema.PostgresDataType;
import lama.postgres.ast.PostgresAggregate;
import lama.postgres.ast.PostgresBetweenOperation;
import lama.postgres.ast.PostgresBinaryOperation;
import lama.postgres.ast.PostgresCastOperation;
import lama.postgres.ast.PostgresCollate;
import lama.postgres.ast.PostgresColumnValue;
import lama.postgres.ast.PostgresConstant;
import lama.postgres.ast.PostgresExpression;
import lama.postgres.ast.PostgresFunction;
import lama.postgres.ast.PostgresInOperation;
import lama.postgres.ast.PostgresJoin;
import lama.postgres.ast.PostgresJoin.PostgresJoinType;
import lama.postgres.ast.PostgresOrderByTerm;
import lama.postgres.ast.PostgresPOSIXRegularExpression;
import lama.postgres.ast.PostgresPostfixOperation;
import lama.postgres.ast.PostgresPostfixText;
import lama.postgres.ast.PostgresPrefixOperation;
import lama.postgres.ast.PostgresSelect;
import lama.postgres.ast.PostgresSimilarTo;

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
	public void visit(PostgresSelect s) {
		sb.append("SELECT ");
		switch (s.getSelectOption()) {
		case DISTINCT:
			sb.append("DISTINCT ");
			if (s.getDistinctOnClause() != null) {
				sb.append("ON (");
				visit(s.getDistinctOnClause());
				sb.append(") ");
			}
			break;
		case ALL:
			sb.append(Randomly.fromOptions("ALL ", ""));
			break;
		default:
			throw new AssertionError();
		}
		if (s.getFetchColumns() == null) {
			sb.append("*");
		} else {
			for (int i = 0; i < s.getFetchColumns().size(); i++) {
				if (i != 0) {
					sb.append(", ");
				}
				PostgresExpression column = s.getFetchColumns().get(i);
				visit(column);
			}
		}
		sb.append(" FROM ");
		for (int i = 0; i < s.getFromList().size(); i++) {
			if (i != 0) {
				sb.append(", ");
			}
			if (s.getFromList().get(i).isOnly()) {
				sb.append("ONLY ");
			}
			sb.append(s.getFromList().get(i).getTable().getName());
			if (!s.getFromList().get(i).isOnly() && Randomly.getBoolean()) {
				sb.append("*");
			}
		}

		for (PostgresJoin j : s.getJoinClauses()) {
			sb.append(" ");
			switch (j.getType()) {
			case INNER:
				if (Randomly.getBoolean()) {
					sb.append("INNER ");
				}
				sb.append("JOIN");
				break;
			case LEFT:
				sb.append("LEFT OUTER JOIN");
				break;
			case RIGHT:
				sb.append("RIGHT OUTER JOIN");
				break;
			case FULL:
				sb.append("FULL OUTER JOIN");
				break;
			case CROSS:
				sb.append("CROSS JOIN");
				break;
			default:
				throw new AssertionError(j.getType());
			}
			sb.append(" ");
			sb.append(j.getTable().getName());
			if (j.getType() != PostgresJoinType.CROSS) {
				sb.append(" ON ");
				visit(j.getOnClause());
			}
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
		if (op.getForClause() != null) {
			sb.append(" FOR ");
			sb.append(op.getForClause());
		}
	}

	public void visit(PostgresFunction f) {
		sb.append(f.getFunctionName());
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
		PostgresCompoundDataType compoundType = cast.getCompoundType();
		switch (compoundType.getDataType()) {
		case BOOLEAN:
			sb.append("BOOLEAN");
			break;
		case INT: // TODO support also other int types
			sb.append("INT");
			break;
		case TEXT:
			// TODO: append TEXT, CHAR
			sb.append(Randomly.fromOptions("VARCHAR"));
			break;
		case REAL:
			sb.append("FLOAT");
			break;
		case DECIMAL:
			sb.append("DECIMAL");
			break;
		case FLOAT:
			sb.append("REAL");
			break;
		case RANGE:
			sb.append("int4range");
			break;
		case MONEY:
			sb.append("MONEY");
			break;
		case INET:
			sb.append("INET");
			break;
		case BIT:
			sb.append("BIT");
//			if (Randomly.getBoolean()) {
//				sb.append("(");
//				sb.append(Randomly.getNotCachedInteger(1, 100));
//				sb.append(")");
//			}
			break;
		default:
			throw new AssertionError(cast.getType());
		}
		Optional<Integer> size = compoundType.getSize();
		if (size.isPresent()) {
			sb.append("(");
			sb.append(size.get());
			sb.append(")");
		}
	}

	@Override
	public void visit(PostgresBinaryOperation op) {
		sb.append("(");
		visit(op.getLeft());
		sb.append(") ");
		sb.append(op.getOperatorTextRepresentation());
		sb.append(" (");
		visit(op.getRight());
		sb.append(")");
	}

	@Override
	public void visit(PostgresBetweenOperation op) {
		sb.append("(");
		visit(op.getExpr());
		if ((op.getExpr().getExpressionType() == PostgresDataType.TEXT
				&& op.getLeft().getExpressionType() == PostgresDataType.TEXT) && PostgresProvider.GENERATE_ONLY_KNOWN) {
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
		if ((op.getExpr().getExpressionType() == PostgresDataType.TEXT
				&& op.getRight().getExpressionType() == PostgresDataType.TEXT) && PostgresProvider.GENERATE_ONLY_KNOWN) {
			sb.append(" COLLATE \"C\"");
		}
		sb.append(")");
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

	@Override
	public void visit(PostgresPostfixText op) {
		visit(op.getExpr());
		sb.append(op.getText());
	}

	@Override
	public void visit(PostgresAggregate op) {
		sb.append(op.getFunc());
		sb.append("(");
		visit(op.getExpr());
		sb.append(")");
	}

	@Override
	public void visit(PostgresSimilarTo op) {
		sb.append("(");
		visit(op.getString());
		sb.append(" SIMILAR TO ");
		visit(op.getSimilarTo());
		if (op.getEscapeCharacter() != null) {
			visit(op.getEscapeCharacter());
		}
		sb.append(")");
	}

	@Override
	public void visit(PostgresPOSIXRegularExpression op) {
		visit(op.getString());
		sb.append(op.getOp().getStringRepresentation());
		visit(op.getRegex());
	}

	@Override
	public void visit(PostgresCollate op) {
		sb.append("(");
		visit(op.getExpr());
		sb.append(" COLLATE ");
		sb.append('"');
		sb.append(op.getCollate());
		sb.append('"');
		sb.append(")");
	}

}
