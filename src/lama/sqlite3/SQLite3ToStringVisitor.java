package lama.sqlite3;

import java.util.List;

import lama.IgnoreMeException;
import lama.Randomly;
import lama.sqlite3.ast.SQLite3Constant;
import lama.sqlite3.ast.SQLite3Expression;
import lama.sqlite3.ast.SQLite3SelectStatement;
import lama.sqlite3.ast.SQLite3Expression.BetweenOperation;
import lama.sqlite3.ast.SQLite3Expression.BinaryOperation;
import lama.sqlite3.ast.SQLite3Expression.Cast;
import lama.sqlite3.ast.SQLite3Expression.CollateOperation;
import lama.sqlite3.ast.SQLite3Expression.ColumnName;
import lama.sqlite3.ast.SQLite3Expression.Exist;
import lama.sqlite3.ast.SQLite3Expression.Function;
import lama.sqlite3.ast.SQLite3Expression.InOperation;
import lama.sqlite3.ast.SQLite3Expression.Join;
import lama.sqlite3.ast.SQLite3Expression.LogicalOperation;
import lama.sqlite3.ast.SQLite3Expression.OrderingTerm;
import lama.sqlite3.ast.SQLite3Expression.PostfixUnaryOperation;
import lama.sqlite3.ast.SQLite3Expression.Subquery;
import lama.sqlite3.ast.SQLite3Expression.TypeLiteral;
import lama.sqlite3.ast.SQLite3Expression.UnaryOperation;
import lama.sqlite3.schema.SQLite3Schema.Column;

public class SQLite3ToStringVisitor extends SQLite3Visitor {

	StringBuffer sb = new StringBuffer();
	
	protected void asHexString(long intVal) {
		String hexVal = Long.toHexString(intVal);
		String prefix;
		if (Randomly.getBoolean()) {
			prefix = "0x";
		} else {
			prefix = "0X";
		}
		sb.append(prefix);
		sb.append(hexVal);
	}
	
	public void visit(BinaryOperation op) {
		sb.append("(");
		sb.append("(");
		visit(op.getLeft());
		sb.append(")");
		sb.append(" ");
		sb.append(op.getOperator().getTextRepresentation());
		sb.append(" ");
		sb.append("(");
		visit(op.getRight());
		sb.append(")");
		sb.append(")");
	}
	
	public void visit(LogicalOperation op) {
		visit(op.getLeft());
		switch (op.getOperator()) {
		case AND:
			sb.append(" AND ");
			break;
		case OR:
			sb.append(" OR ");
		}
		visit(op.getRight());
	}
	
	public void visit(BetweenOperation op) {
		visit(op.getExpression());
		if (op.isNegated()) {
			sb.append(" NOT");
		}
		sb.append(" BETWEEN ");
		visit(op.getLeft());
		sb.append(" AND ");
		visit(op.getRight());
	}
	
	public void visit(ColumnName c) {
		if (fullyQualifiedNames) {
			sb.append(c.getColumn().getTable().getName());
			sb.append('.');
		}
		sb.append(c.getColumn().getName());
	}
	
	public void visit(Function f) {
		sb.append(f.getName());
		sb.append("(");
		for (int i = 0; i < f.getArguments().length; i++) {
			if (i != 0) {
				sb.append(", ");
			}
			visit(f.getArguments()[i]);
		}
		sb.append(")");
	}
	
	public void visit(SQLite3SelectStatement s) {
		sb.append("SELECT ");
		switch (s.getFromOptions()) {
		case DISTINCT:
			sb.append("DISTINCT ");
			break;
		case ALL:
			sb.append(Randomly.fromOptions("ALL ", ""));
			break;
		}
		if (s.getFetchColumns() == null) {
			sb.append("*");
		} else {
			for (int i = 0; i < s.getFetchColumns().size(); i++) {
				if (i != 0) {
					sb.append(", ");
				}
				Column column = s.getFetchColumns().get(i);
				sb.append(column.getTable().getName());
				sb.append('.');
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
		for (Join j : s.getJoinClauses()) {
			visit(j);
		}

		if (s.getWhereClause() != null) {
			SQLite3Expression whereClause = s.getWhereClause();
			sb.append(" WHERE ");
			visit(whereClause);
		}
		if (s.getGroupByClause() != null && s.getGroupByClause().size() > 0) {
			sb.append(" ");
			sb.append("GROUP BY ");
			List<SQLite3Expression> groupBys = s.getGroupByClause();
			for (int i = 0; i < groupBys.size(); i++) {
				if (i != 0) {
					sb.append(", ");
				}
				visit(groupBys.get(i));
			}
		}
		if (!s.getOrderByClause().isEmpty()) {
			sb.append(" ORDER BY ");
			List<SQLite3Expression> orderBys = s.getOrderByClause();
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
	
	public void visit(SQLite3Constant c) {
		if (c.isNull()) {
			sb.append("NULL");
		} else {
			switch (c.getDataType()) {
			case INT:
				if ((c.asInt() == 0 || c.asInt() == 1) && Randomly.getBoolean()) {
					sb.append(c.asInt() == 1 ? "TRUE" : "FALSE");
				} else {
					// - 0X8000000000000000 results in an error message otherwise
					if (Randomly.getBoolean() || c.asInt() == Long.MIN_VALUE) {
						sb.append(c.asInt());
					} else {
						long intVal = c.asInt();
						asHexString(intVal); 
					}
				}
				break;
			case REAL:
				double asDouble = c.asDouble();
				if (Double.POSITIVE_INFINITY == asDouble) {
					sb.append("1e500");
				} else if (Double.NEGATIVE_INFINITY == asDouble) {
					sb.append("-1e500");
				} else if (Double.isNaN(asDouble)) {
					throw new IgnoreMeException();
//					sb.append("1e500 / 1e500");
				} else {
					sb.append(asDouble);
				}
				break;
			case TEXT:
				sb.append("'");
				sb.append(c.asString().replace("'", "''"));
				sb.append("'");
				break;
			case BINARY:
				sb.append('x');
				sb.append("'");
				byte[] arr;
				if (c.getValue() instanceof byte[]) {
					arr = c.asBinary();
				} else {
					arr = c.asString().getBytes();
				}
				sb.append(byteArrayToHex(arr));
				sb.append("'");
				break;
			default:
				throw new AssertionError(c.getDataType());
			}
		}
	}
	
	public void visit(Join join) {
		sb.append(" ");
		switch (join.getType()) {
		case CROSS:
			sb.append("CROSS");
			break;
		case INNER:
			sb.append("INNER");
			break;
		case NATURAL:
			sb.append("NATURAL");
			break;
		case OUTER:
			sb.append("LEFT OUTER");
			break;
		default:
			throw new AssertionError(join.getType());
		}
		sb.append(" JOIN ");
		sb.append(join.getTable().getName());
		sb.append(" ON ");
		visit(join.getOnClause());
	}
	
	public void visit(OrderingTerm term) {
		visit(term.getExpression());
		// TODO make order optional?
		sb.append(" ");
		sb.append(term.getOrdering().toString());
	}

	public void visit(UnaryOperation exp) {
		sb.append("(");
		sb.append(exp.getOperation().getTextRepresentation());
		sb.append(" ");
		visit(exp.getExpression());
		sb.append(")");
	}

	public void visit(PostfixUnaryOperation exp) {
		sb.append("(");
		visit(exp.getExpression());
		sb.append(" ");
		sb.append(exp.getOperation().getTextRepresentation());
		sb.append(")");
	}

	public void visit(CollateOperation op) {
		visit(op.getExpression());
		sb.append(" COLLATE ");
		sb.append(op.getCollate());
	}

	public void visit(Cast cast) {
		sb.append("CAST(");
		visit(cast.getExpression());
		sb.append(" AS ");
		visit(cast.getType());
		sb.append(")");
	}

	public void visit(TypeLiteral literal) {
		sb.append(literal.getType());
	}

	public void visit(InOperation op) {
		visit(op.getLeft());
		sb.append(" IN ");
		sb.append("(");
		for (int i = 0; i < op.getRight().size(); i++) {
			if (i != 0) {
				sb.append(", ");
			}
			visit(op.getRight().get(i));
		}
		sb.append(")");
	}

	public void visit(Subquery query) {
		sb.append(query.getQuery());
	}

	public void visit(Exist exist) {
		sb.append(" EXISTS (");
		visit(exist.getSelect());
		sb.append(")");
	}

	
	public String get() {
		return sb.toString();
	}
}
