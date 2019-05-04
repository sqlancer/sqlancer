package lama.sqlite3;

import java.util.List;

import javax.xml.bind.DatatypeConverter;

import lama.Expression;
import lama.Expression.BetweenOperation;
import lama.Expression.BinaryOperation;
import lama.Expression.CollateOperation;
import lama.Expression.ColumnName;
import lama.Expression.Constant;
import lama.Expression.Function;
import lama.Expression.InOperation;
import lama.Expression.LogicalOperation;
import lama.Expression.OrderingTerm;
import lama.Expression.PostfixUnaryOperation;
import lama.Expression.UnaryOperation;
import lama.Randomly;
import lama.SelectStatement;

public class SQLite3Visitor {

	StringBuffer sb = new StringBuffer();

	// see
	// https://www.mail-archive.com/sqlite-users@mailinglists.sqlite.org/msg115014.html
	private boolean stringsAsDoubleQuotes;

	void visit(BinaryOperation op) {
		sb.append("(");
		visit(op.getLeft());
		sb.append(" ");
		sb.append(op.getOperator().getTextRepresentation());
		sb.append(" ");
		visit(op.getRight());
		sb.append(")");
	}

	void visit(LogicalOperation op) {
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

	void visit(BetweenOperation op) {
		visit(op.getExpression());
		if (op.isNegated()) {
			sb.append(" NOT");
		}
		sb.append(" BETWEEN ");
		visit(op.getLeft());
		sb.append(" AND ");
		visit(op.getRight());
	}

	void visit(ColumnName c) {
		sb.append(c.getColumn().getName());
	}

	public void setStringsAsDoubleQuotes(boolean stringsAsDoubleQuotes) {
		this.stringsAsDoubleQuotes = stringsAsDoubleQuotes;
	}

	void visit(Constant c) {
		if (c.isNull()) {
			sb.append("NULL");
		} else {
			switch (c.getDataType()) {
			case INT:
				if (c.getValue() instanceof Boolean) {
					sb.append(c.asBoolean() ? "TRUE" : "FALSE");
				} else {
					if (Randomly.getBoolean()) {
						sb.append(c.asInt());
					} else {
						long intVal = c.asInt();
						asHexString(intVal);
					}
				}
				break;
			case REAL:
				sb.append(c.asDouble());
				break;
			case TEXT:
				// not escape with double quotes
				// CREATE TABLE test (c0, c1);
				// INSERT INTO test(c0, c1) VALUES ("c1", 0);
				// SELECT * FROM test WHERE c0 = 'c1';
				// yields no results
				String quotes;
				if (stringsAsDoubleQuotes) {
					quotes = "\"";
				} else {
					quotes = "'";
				}
				sb.append(quotes);
				sb.append(c.asString().replace("'", "''"));
				sb.append(quotes);
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
				sb.append(DatatypeConverter.printHexBinary(arr));
				sb.append("'");
				break;
			default:
				throw new AssertionError(c.getDataType());
			}
		}
	}

	private void asHexString(long intVal) {
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

	public void visit(SelectStatement s) {
		sb.append("SELECT ");
		switch (s.getFromOptions()) {
		case DISTINCT:
			sb.append("DISTINCT ");
			break;
		case ALL:
			sb.append(Randomly.fromOptions("ALL ", ""));
			break;
		}
		sb.append("* ");
		sb.append("FROM ");
		for (int i = 0; i < s.getFromList().size(); i++) {
			if (i != 0) {
				sb.append(", ");
			}
			sb.append(s.getFromList().get(i).getName());
		}
		if (s.getWhereClause() != null) {
			Expression whereClause = s.getWhereClause();
			sb.append(" WHERE ");
			visit(whereClause);
		}
		if (s.getGroupByClause() != null && s.getGroupByClause().size() > 0) {
			sb.append(" ");
			sb.append("GROUP BY ");
			List<Expression> groupBys = s.getGroupByClause();
			for (int i = 0; i < groupBys.size(); i++) {
				if (i != 0) {
					sb.append(", ");
				}
				visit(groupBys.get(i));
			}
		}
		if (!s.getOrderByClause().isEmpty()) {
			sb.append(" ORDER BY ");
			List<Expression> orderBys = s.getOrderByClause();
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

	private void visit(OrderingTerm term) {
		visit(term.getExpression());
		// TODO make order optional?
		sb.append(" ");
		sb.append(term.getOrdering().toString());
	}

	private void visit(UnaryOperation exp) {
		sb.append(exp.getOperation().getTextRepresentation());
		sb.append(" ");
		visit(exp.getExpression());
	}

	private void visit(PostfixUnaryOperation exp) {
		visit(exp.getExpression());
		sb.append(" ");
		sb.append(exp.getOperation().getTextRepresentation());
	}
	
	private void visit(CollateOperation op) {
		visit(op.getExpression());
		sb.append(" COLLATE ");
		sb.append(op.getCollate());
	}
	
	private void visit(InOperation op) {
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

	public void visit(Expression expr) {
		if (expr instanceof BinaryOperation) {
			visit((BinaryOperation) expr);
		} else if (expr instanceof LogicalOperation) {
			visit((LogicalOperation) expr);
		} else if (expr instanceof ColumnName) {
			visit((ColumnName) expr);
		} else if (expr instanceof Constant) {
			visit((Constant) expr);
		} else if (expr instanceof UnaryOperation) {
			visit((UnaryOperation) expr);
		} else if (expr instanceof PostfixUnaryOperation) {
			visit((PostfixUnaryOperation) expr);
		} else if (expr instanceof Function) {
			visit((Function) expr);
		} else if (expr instanceof BetweenOperation) {
			visit((BetweenOperation) expr);
		} else if (expr instanceof CollateOperation) {
			visit((CollateOperation) expr);
		} else if (expr instanceof OrderingTerm) {
			visit((OrderingTerm) expr);
		} else if (expr instanceof Expression.InOperation) {
			visit((InOperation) expr);
		} else {
			throw new AssertionError(expr);
		}
	}

	public String get() {
		return sb.toString();
	}

	public static String asString(Expression expr) {
		SQLite3Visitor visitor = new SQLite3Visitor();
		visitor.visit(expr);
		return visitor.get();
	}

}
