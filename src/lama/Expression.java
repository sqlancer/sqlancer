package lama;

import java.util.Arrays;

import lama.schema.SQLite3DataType;
import lama.schema.Schema.Column;

public class Expression {

	public static class BetweenOperation extends Expression {

		private final Expression expr;
		private final boolean negated;
		private final Expression left;
		private final Expression right;

		public BetweenOperation(Expression expr, boolean negated, Expression left, Expression right) {
			this.expr = expr;
			this.negated = negated;
			this.left = left;
			this.right = right;
		}

		public Expression getExpression() {
			return expr;
		}

		public boolean isNegated() {
			return negated;
		}

		public Expression getLeft() {
			return left;
		}

		public Expression getRight() {
			return right;
		}

	}

	public static class Function extends Expression {

		private final Expression[] arguments;
		private final String name;

		public Function(String name, Expression... arguments) {
			this.name = name;
			this.arguments = arguments;
		}

		public Expression[] getArguments() {
			return arguments;
		}

		public String getName() {
			return name;
		}

	}

	public static class OrderingTerm extends Expression {

		private final Expression expression;
		private final Ordering ordering;

		public enum Ordering {
			ASC, DESC
		}

		public OrderingTerm(Expression expression, Ordering ordering) {
			this.expression = expression;
			this.ordering = ordering;
		}

		public Expression getExpression() {
			return expression;
		}

		public Ordering getOrdering() {
			return ordering;
		}

	}

	public static class UnaryOperation extends Expression {

		/**
		 * Supported unary prefix operators are these:
		 * 
		 * - + ~ NOT
		 * 
		 * @see https://www.sqlite.org/lang_expr.html
		 *
		 */
		public enum UnaryOperator {
			MINUS("-"), PLUS("+"), NEGATE("~"), NOT("NOT");

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

		}

		private final UnaryOperator operation;
		private final Expression expression;

		public UnaryOperation(UnaryOperator operation, Expression expression) {
			this.operation = operation;
			this.expression = expression;
		}

		public UnaryOperator getOperation() {
			return operation;
		}

		public Expression getExpression() {
			return expression;
		}

	}

	public static class CollateOperation extends Expression {

		private final Expression expression;
		private final String collate;

		public CollateOperation(Expression expression, String collate) {
			this.expression = expression;
			this.collate = collate;
		}

		public String getCollate() {
			return collate;
		}

		public Expression getExpression() {
			return expression;
		}

	}

	public static class PostfixUnaryOperation extends Expression {

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
		private final Expression expression;

		public PostfixUnaryOperation(PostfixUnaryOperator operation, Expression expression) {
			this.operation = operation;
			this.expression = expression;
		}

		public PostfixUnaryOperator getOperation() {
			return operation;
		}

		public Expression getExpression() {
			return expression;
		}

	}

	public static class BinaryOperation extends Expression {

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
		private final Expression left;
		private final Expression right;

		public BinaryOperation(Expression left, Expression right, BinaryOperator operation) {
			this.left = left;
			this.right = right;
			this.operation = operation;
		}

		public BinaryOperator getOperator() {
			return operation;
		}

		public Expression getLeft() {
			return left;
		}

		public Expression getRight() {
			return right;
		}

	}

	public static class LogicalOperation extends Expression {

		private final Expression left;
		private final Expression right;
		private final LogicalOperator operator;

		public LogicalOperation(Expression left, Expression right, LogicalOperator operator) {
			this.left = left;
			this.right = right;
			this.operator = operator;
		}

		public Expression getLeft() {
			return left;
		}

		public Expression getRight() {
			return right;
		}

		public LogicalOperator getOperator() {
			return operator;
		}

		public enum LogicalOperator {
			AND, OR
		}

	}

	public static class ColumnName extends Expression {

		private final Column column;

		public ColumnName(Column name) {
			this.column = name;
		}

		public Column getColumn() {
			return column;
		}

	}

	public static class Constant extends Expression {

		private final Object value;
		private final SQLite3DataType dataType;

		private Constant(Object value, SQLite3DataType dataType) {
			if (dataType == null || value instanceof Constant) {
				throw new IllegalArgumentException();
			}
			this.value = value;
			this.dataType = dataType;
		}

		public boolean isNull() {
			return value == null;
		}

		public long asInt() {
			return (long) value;
		}

		public double asDouble() {
			return (double) value;
		}

		public String asString() {
			// TODO Fixme and use only byte[]
			if (value instanceof byte[]) {
				return Arrays.toString((byte[]) value);
			} else {
				return value.toString();
			}
		}

		public SQLite3DataType getDataType() {
			return dataType;
		}

		public String asDate() {
			return (String) value;
		}

		public Object getValue() {
			return value;
		}

		public boolean asBoolean() {
			return (Boolean) value;
		}

		public byte[] asBinary() {
			return (byte[]) value;
		}

		public static Constant createIntConstant(long val) {
			return new Constant(val, SQLite3DataType.INT);
		}

		public static Constant createBinaryConstant(byte[] val) {
			return new Constant(val, SQLite3DataType.BINARY);
		}

		public static Constant createRealConstant(double real) {
			return new Constant(real, SQLite3DataType.REAL);
		}

		public static Constant createTextConstant(String text) {
			return new Constant(text, SQLite3DataType.TEXT);
		}

		public static Constant createNullConstant() {
			return new Constant(null, SQLite3DataType.NULL);
		}

		public static Constant getRandomBinaryConstant() {
			int size = Randomly.smallNumber();
			byte[] arr = new byte[size];
			Randomly.getBytes(arr);
			return new Constant(arr, SQLite3DataType.BINARY);
		}

		@Override
		public String toString() {
			return String.format("(%s) %s", dataType,
					value instanceof byte[] ? Arrays.toString((byte[]) value) : value);
		}

		@Deprecated
		public static Constant create(Object value2, SQLite3DataType valueType) {
			return new Constant(value2, valueType);
		}

		public static Expression createBooleanConstant(boolean val) {
			return new Constant(val, SQLite3DataType.INT);
		}


	}

}
