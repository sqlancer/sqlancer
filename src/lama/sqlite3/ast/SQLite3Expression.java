package lama.sqlite3.ast;

import java.util.List;
import java.util.Optional;

import lama.Randomly;
import lama.sqlite3.gen.SQLite3Cast;
import lama.sqlite3.schema.SQLite3DataType;
import lama.sqlite3.schema.SQLite3Schema.Column;
import lama.sqlite3.schema.SQLite3Schema.Table;

public class SQLite3Expression {

	public SQLite3Constant getExpectedValue() {
		return null;
	}

	public static class Exist extends SQLite3Expression {

		private final SQLite3SelectStatement select;

		public Exist(SQLite3SelectStatement select) {
			this.select = select;
		}

		public SQLite3SelectStatement getSelect() {
			return select;
		}

	}

	public static class Join extends SQLite3Expression {

		public static enum JoinType {
			INNER, CROSS, OUTER, NATURAL;
		}

		private final Table table;
		private final SQLite3Expression onClause;
		private final JoinType type;

		public Join(Table table, SQLite3Expression onClause, JoinType type) {
			this.table = table;
			this.onClause = onClause;
			this.type = type;
		}

		public Table getTable() {
			return table;
		}

		public SQLite3Expression getOnClause() {
			return onClause;
		}

		public JoinType getType() {
			return type;
		}

	}

	public static class Subquery extends SQLite3Expression {

		private final String query;

		public Subquery(String query) {
			this.query = query;
		}

		public static SQLite3Expression create(String query) {
			return new Subquery(query);
		}

		public String getQuery() {
			return query;
		}

	}

	public static class TypeLiteral {

		private final Type type;

		public static enum Type {
			TEXT {
				@Override
				public SQLite3Constant apply(SQLite3Constant cons) {
					// TODO Auto-generated method stub
					return null;
				}
			}, REAL {
				@Override
				public SQLite3Constant apply(SQLite3Constant cons) {
					// TODO Auto-generated method stub
					return null;
				}
			}, INTEGER {
				@Override
				public SQLite3Constant apply(SQLite3Constant cons) {
					// TODO Auto-generated method stub
					return null;
				}
			}, NUMERIC {
				@Override
				public SQLite3Constant apply(SQLite3Constant cons) {
					return SQLite3Cast.castToNumeric(cons);
				}
			}, BINARY {
				@Override
				public SQLite3Constant apply(SQLite3Constant cons) {
					// TODO Auto-generated method stub
					return null;
				}
			};
			
			public abstract SQLite3Constant apply(SQLite3Constant cons);
		}

		public TypeLiteral(Type type) {
			this.type = type;
		}

		public Type getType() {
			return type;
		}
		
	}

	public static class Cast extends SQLite3Expression {

		private final TypeLiteral type;
		private final SQLite3Expression expression;

		public Cast(TypeLiteral typeofExpr, SQLite3Expression expression) {
			this.type = typeofExpr;
			this.expression = expression;
		}

		public SQLite3Expression getExpression() {
			return expression;
		}

		public TypeLiteral getType() {
			return type;
		}
		
		@Override
		public SQLite3Constant getExpectedValue() {
			if (expression.getExpectedValue() == null) {
				return null;
			} else {
				return type.type.apply(expression.getExpectedValue());
			}
		}

	}

	public static class BetweenOperation extends SQLite3Expression {

		private final SQLite3Expression expr;
		private final boolean negated;
		private final SQLite3Expression left;
		private final SQLite3Expression right;

		public BetweenOperation(SQLite3Expression expr, boolean negated, SQLite3Expression left,
				SQLite3Expression right) {
			this.expr = expr;
			this.negated = negated;
			this.left = left;
			this.right = right;
		}

		public SQLite3Expression getExpression() {
			return expr;
		}

		public boolean isNegated() {
			return negated;
		}

		public SQLite3Expression getLeft() {
			return left;
		}

		public SQLite3Expression getRight() {
			return right;
		}

	}

	public static class Function extends SQLite3Expression {

		private final SQLite3Expression[] arguments;
		private final String name;

		public Function(String name, SQLite3Expression... arguments) {
			this.name = name;
			this.arguments = arguments;
		}

		public SQLite3Expression[] getArguments() {
			return arguments;
		}

		public String getName() {
			return name;
		}

	}

	public static class OrderingTerm extends SQLite3Expression {

		private final SQLite3Expression expression;
		private final Ordering ordering;

		public enum Ordering {
			ASC, DESC
		}

		public OrderingTerm(SQLite3Expression expression, Ordering ordering) {
			this.expression = expression;
			this.ordering = ordering;
		}

		public SQLite3Expression getExpression() {
			return expression;
		}

		public Ordering getOrdering() {
			return ordering;
		}

	}

	public static class UnaryOperation extends SQLite3Expression {

		/**
		 * Supported unary prefix operators are these:
		 * 
		 * - + ~ NOT
		 * 
		 * @see https://www.sqlite.org/lang_expr.html
		 *
		 */
		public enum UnaryOperator {
			MINUS("-") {
				@Override
				public SQLite3Constant apply(SQLite3Constant constant) {
					if (constant.isNull()) {
						return SQLite3Constant.createNullConstant();
					}
					if (constant.getDataType() == SQLite3DataType.TEXT
							|| constant.getDataType() == SQLite3DataType.BINARY) {
						constant = SQLite3Cast.castToNumeric(constant);
					}
					if (constant.getDataType() == SQLite3DataType.INT) {
						return SQLite3Constant.createIntConstant(-constant.asInt());
					}
					if (constant.getDataType() == SQLite3DataType.REAL) {
						return SQLite3Constant.createRealConstant(-constant.asDouble());
					}
					throw new AssertionError(constant);
				}
			},
			PLUS("+") {
				@Override
				public SQLite3Constant apply(SQLite3Constant constant) {
					return constant;
				}
			},
			NEGATE("~") {
				@Override
				public SQLite3Constant apply(SQLite3Constant constant) {
					// TODO implement me
					return null;
				}
			},
			NOT("NOT") {
				@Override
				public SQLite3Constant apply(SQLite3Constant constant) {
					Optional<Boolean> boolVal = SQLite3Cast.isTrue(constant);
					if (boolVal.isPresent()) {
						Boolean negated = !boolVal.get();
						return SQLite3Constant.createBoolean(negated);
					} else {
						return SQLite3Constant.createNullConstant();
					}
				}
			};

			private String textRepresentation;

			private UnaryOperator(String textRepresentation) {
				this.textRepresentation = textRepresentation;
			}

			@Override
			public String toString() {
				return getTextRepresentation();
			}

			public String getTextRepresentation() {
				return textRepresentation;
			}

			public UnaryOperator getRandomOperator() {
				return Randomly.fromOptions(values());
			}

			public abstract SQLite3Constant apply(SQLite3Constant constant);

		}

		private final UnaryOperator operation;
		private final SQLite3Expression expression;

		public UnaryOperation(UnaryOperator operation, SQLite3Expression expression) {
			this.operation = operation;
			this.expression = expression;
		}

		public UnaryOperator getOperation() {
			return operation;
		}

		public SQLite3Expression getExpression() {
			return expression;
		}

		@Override
		public SQLite3Constant getExpectedValue() {
			if (expression.getExpectedValue() == null) {
				return null;
			} else {
				return operation.apply(expression.getExpectedValue());
			}
		}

	}

	public static class CollateOperation extends SQLite3Expression {

		private final SQLite3Expression expression;
		private final String collate;

		public CollateOperation(SQLite3Expression expression, String collate) {
			this.expression = expression;
			this.collate = collate;
		}

		public String getCollate() {
			return collate;
		}

		public SQLite3Expression getExpression() {
			return expression;
		}

	}

	public static class PostfixUnaryOperation extends SQLite3Expression {

		public enum PostfixUnaryOperator {
			ISNULL("ISNULL"), NOT_NULL("NOT NULL"), NOTNULL("NOTNULL");

			private final String textRepresentation;

			private PostfixUnaryOperator(String textRepresentation) {
				this.textRepresentation = textRepresentation;
			}

			@Override
			public String toString() {
				return getTextRepresentation();
			}

			public String getTextRepresentation() {
				return textRepresentation;
			}

			public static PostfixUnaryOperator getRandomOperator() {
				return Randomly.fromOptions(values());
			}

		}

		private final PostfixUnaryOperator operation;
		private final SQLite3Expression expression;

		public PostfixUnaryOperation(PostfixUnaryOperator operation, SQLite3Expression expression) {
			this.operation = operation;
			this.expression = expression;
		}

		public PostfixUnaryOperator getOperation() {
			return operation;
		}

		public SQLite3Expression getExpression() {
			return expression;
		}

		@Override
		public SQLite3Constant getExpectedValue() {
			if (expression.getExpectedValue() == null) {
				return null;
			}
			switch (operation) {
			case ISNULL:
				if (expression.getExpectedValue().isNull()) {
					return SQLite3Constant.createTrue();
				} else {
					return SQLite3Constant.createFalse();
				}
			case NOT_NULL:
			case NOTNULL:
				if (expression.getExpectedValue().isNull()) {
					return SQLite3Constant.createFalse();
				} else {
					return SQLite3Constant.createTrue();
				}
			default:
				throw new AssertionError(operation);
			}
		}

	}

	public static class InOperation extends SQLite3Expression {

		private final SQLite3Expression left;
		private final List<SQLite3Expression> right;

		public InOperation(SQLite3Expression left, List<SQLite3Expression> right) {
			this.left = left;
			this.right = right;
		}

		public SQLite3Expression getLeft() {
			return left;
		}

		public List<SQLite3Expression> getRight() {
			return right;
		}
	}

	public static class BinaryOperation extends SQLite3Expression {

		public enum BinaryOperator {
			CONCATENATE("||"), MULTIPLY("*"), DIVIDE("/"), // division by zero results in zero
			REMAINDER("%"), PLUS("+"), MINUS("-"), SHIFT_LEFT("<<"), SHIFT_RIGHT(">>"), ARITHMETIC_AND("&"),
			ARITHMETIC_OR("|"), SMALLER("<"), SMALLER_EQUALS("<="), GREATER(">"), GREATER_EQUALS(">="),
			EQUALS("=", "=="), NOT_EQUALS("!=", "<>"), IS("IS"), IS_NOT("IS NOT"),
			// IN("IN"),
			LIKE("LIKE"), GLOB("GLOB"),
			// MATCH("MATCH"),
			// REGEXP("REGEXP"),
			AND("AND"), OR("OR");

			private final String textRepresentation[];

			private BinaryOperator(String... textRepresentation) {
				this.textRepresentation = textRepresentation;
			}

			public static BinaryOperator getRandomOperator() {
				return Randomly.fromOptions(values());
			}

			public String getTextRepresentation() {
				return Randomly.fromOptions(textRepresentation);
			}

			public BinaryOperator reverse() {
				switch (this) {
				case IS:
					return IS_NOT;
				case IS_NOT:
					return IS;
				case EQUALS:
					return NOT_EQUALS;
				case NOT_EQUALS:
					return EQUALS;
				case GREATER:
					return SMALLER_EQUALS;
				case GREATER_EQUALS:
					return SMALLER;
				case SMALLER_EQUALS:
					return GREATER;
				case SMALLER:
					return GREATER_EQUALS;
				default:
					throw new AssertionError(this);
				}
			}
		}

		private final BinaryOperator operation;
		private final SQLite3Expression left;
		private final SQLite3Expression right;

		public BinaryOperation(SQLite3Expression left, SQLite3Expression right, BinaryOperator operation) {
			this.left = left;
			this.right = right;
			this.operation = operation;
		}

		public BinaryOperator getOperator() {
			return operation;
		}

		public SQLite3Expression getLeft() {
			return left;
		}

		public SQLite3Expression getRight() {
			return right;
		}

	}

	public static class LogicalOperation extends SQLite3Expression {

		private final SQLite3Expression left;
		private final SQLite3Expression right;
		private final LogicalOperator operator;

		public LogicalOperation(SQLite3Expression left, SQLite3Expression right, LogicalOperator operator) {
			this.left = left;
			this.right = right;
			this.operator = operator;
		}

		public SQLite3Expression getLeft() {
			return left;
		}

		public SQLite3Expression getRight() {
			return right;
		}

		public LogicalOperator getOperator() {
			return operator;
		}

		public enum LogicalOperator {
			AND, OR
		}

	}

	public static class ColumnName extends SQLite3Expression {

		private final Column column;
		private final SQLite3Constant value;

		public ColumnName(Column name, SQLite3Constant value) {
			this.column = name;
			this.value = value;
		}

		public Column getColumn() {
			return column;
		}

		@Override
		public SQLite3Constant getExpectedValue() {
			return value;
		}

	}

}
