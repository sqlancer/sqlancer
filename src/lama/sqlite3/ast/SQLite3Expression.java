package lama.sqlite3.ast;

import java.util.List;
import java.util.Optional;

import lama.Randomly;
import lama.sqlite3.ast.SQLite3Constant.SQLite3IntConstant;
import lama.sqlite3.gen.SQLite3Cast;
import lama.sqlite3.schema.SQLite3DataType;
import lama.sqlite3.schema.SQLite3Schema.Column;
import lama.sqlite3.schema.SQLite3Schema.Column.CollateSequence;
import lama.sqlite3.schema.SQLite3Schema.Table;

public abstract class SQLite3Expression {

	public SQLite3Constant getExpectedValue() {
		return null;
	}

	public static enum TypeAffinity {
		INTEGER, TEXT, BLOB, REAL, NUMERIC, NONE;

		public boolean isNumeric() {
			return this == INTEGER || this == REAL || this == NUMERIC;
		}
	}

	/**
	 * See https://www.sqlite.org/datatype3.html 3.2
	 */
	public TypeAffinity getAffinity() {
		return TypeAffinity.NONE;
	}

	/**
	 * See
	 * https://www.sqlite.org/datatype3.html#assigning_collating_sequences_from_sql
	 * 7.1
	 * 
	 * @return
	 */
	public abstract CollateSequence getExplicitCollateSequence();

	public CollateSequence getImplicitCollateSequence() {
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

		@Override
		public CollateSequence getExplicitCollateSequence() {
			return null;
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

		@Override
		public CollateSequence getExplicitCollateSequence() {
			return null;
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

		@Override
		public CollateSequence getExplicitCollateSequence() {
			return null;
		}

	}

	public static class TypeLiteral {

		private final Type type;

		public static enum Type {
			TEXT {
				@Override
				public SQLite3Constant apply(SQLite3Constant cons) {
					return null;
//					return SQLite3Cast.castToText(cons);
				}
			},
			REAL {
				@Override
				public SQLite3Constant apply(SQLite3Constant cons) {
					return SQLite3Cast.castToReal(cons);
				}
			},
			INTEGER {
				@Override
				public SQLite3Constant apply(SQLite3Constant cons) {
					return SQLite3Cast.castToInt(cons);
				}
			},
			NUMERIC {
				@Override
				public SQLite3Constant apply(SQLite3Constant cons) {
					return SQLite3Cast.castToNumeric(cons);
				}
			},
			BLOB {
				@Override
				public SQLite3Constant apply(SQLite3Constant cons) {
					return SQLite3Cast.castToBlob(cons);
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

		@Override
		/**
		 * An expression of the form "CAST(expr AS type)" has an affinity that is the
		 * same as a column with a declared type of "type".
		 */
		public TypeAffinity getAffinity() {
			switch (type.type) {
			case BLOB:
				return TypeAffinity.BLOB;
			case INTEGER:
				return TypeAffinity.INTEGER;
			case NUMERIC:
				return TypeAffinity.NUMERIC;
			case REAL:
				return TypeAffinity.REAL;
			case TEXT:
				return TypeAffinity.TEXT;
			default:
				throw new AssertionError();
			}
		}

		@Override
		public CollateSequence getExplicitCollateSequence() {
			return expression.getExplicitCollateSequence();
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

		@Override
		public CollateSequence getExplicitCollateSequence() {
			if (expr.getExplicitCollateSequence() != null) {
				return expr.getExplicitCollateSequence();
			} else if (left.getExplicitCollateSequence() != null) {
				return left.getExplicitCollateSequence();
			} else {
				return right.getExplicitCollateSequence();
			}
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

		@Override
		public CollateSequence getExplicitCollateSequence() {
			for (SQLite3Expression arg : arguments) {
				if (arg.getExplicitCollateSequence() != null) {
					return arg.getExplicitCollateSequence();
				}
			}
			return null;
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

		@Override
		public CollateSequence getExplicitCollateSequence() {
			return expression.getExplicitCollateSequence();
		}

	}

	public static class CollateOperation extends SQLite3Expression {

		private final SQLite3Expression expression;
		private final CollateSequence collate;

		public CollateOperation(SQLite3Expression expression, CollateSequence collate) {
			this.expression = expression;
			this.collate = collate;
		}

		public CollateSequence getCollate() {
			return collate;
		}

		public SQLite3Expression getExpression() {
			return expression;
		}

		// If either operand has an explicit collating function assignment using the
		// postfix COLLATE operator, then the explicit collating function is used for
		// comparison, with precedence to the collating function of the left operand.
		@Override
		public CollateSequence getExplicitCollateSequence() {
			return collate;
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

		@Override
		public CollateSequence getExplicitCollateSequence() {
			return expression.getExplicitCollateSequence();
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

		@Override
		public CollateSequence getExplicitCollateSequence() {
			if (left.getExplicitCollateSequence() != null) {
				return left.getExplicitCollateSequence();
			} else {
				for (SQLite3Expression expr : getRight()) {
					if (expr.getExplicitCollateSequence() != null) {
						return expr.getExplicitCollateSequence();
					}
				}
				return null;
			}
		}
	}

	public static class BinaryComparisonOperation extends SQLite3Expression {

		private final BinaryComparisonOperator operation;
		private final SQLite3Expression left;
		private final SQLite3Expression right;

		public BinaryComparisonOperation(SQLite3Expression left, SQLite3Expression right,
				BinaryComparisonOperator operation) {
			this.left = left;
			this.right = right;
			this.operation = operation;
		}

		public BinaryComparisonOperator getOperator() {
			return operation;
		}

		public SQLite3Expression getLeft() {
			return left;
		}

		public SQLite3Expression getRight() {
			return right;
		}

		@Override
		public SQLite3Constant getExpectedValue() {
			if (left.getExpectedValue() == null || right.getExpectedValue() == null) {
				return null;
			}
			return operation.applyOperand(left.getExpectedValue(), left.getAffinity(), right.getExpectedValue(),
					right.getAffinity());
		}

		public static BinaryComparisonOperation create(SQLite3Expression leftVal, SQLite3Expression rightVal,
				BinaryComparisonOperator op) {
			return new BinaryComparisonOperation(leftVal, rightVal, op);
		}

		public enum BinaryComparisonOperator {
			SMALLER("<"), SMALLER_EQUALS("<="), GREATER(">"), GREATER_EQUALS(">="), EQUALS("=", "==") {
				@Override
				SQLite3Constant apply(SQLite3Constant left, SQLite3Constant right, CollateSequence collate) {
					return left.applyEquals(right, collate);
				}

			},
			NOT_EQUALS("!=", "<>") {
				@Override
				SQLite3Constant apply(SQLite3Constant left, SQLite3Constant right, CollateSequence collate) {
					if (left == null || right == null) {
						return null;
					}
					if (left.isNull() || right.isNull()) {
						return SQLite3Constant.createNullConstant();
					} else {
						SQLite3Constant applyEquals = left.applyEquals(right, collate);
						if (applyEquals == null) {
							return null;
						}
						boolean equals = applyEquals.asInt() == 1;
						return SQLite3Constant.createBoolean(!equals);
					}
				}

			},
			IS("IS") {
				@Override
				SQLite3Constant apply(SQLite3Constant left, SQLite3Constant right, CollateSequence collate) {
					if (left == null || right == null) {
						return null;
					} else if (left.isNull()) {
						return SQLite3Constant.createBoolean(right.isNull());
					} else if (right.isNull()) {
						return SQLite3Constant.createFalse();
					} else {
						return left.applyEquals(right, collate);
					}
				}

			},
			IS_NOT("IS NOT") {
				@Override
				SQLite3Constant apply(SQLite3Constant left, SQLite3Constant right, CollateSequence collate) {
					if (left == null || right == null) {
						return null;
					} else if (left.isNull()) {
						return SQLite3Constant.createBoolean(!right.isNull());
					} else if (right.isNull()) {
						return SQLite3Constant.createTrue();
					} else {
						SQLite3Constant applyEquals = left.applyEquals(right, collate);
						if (applyEquals == null) {
							return null;
						}
						boolean equals = applyEquals.asInt() == 1;
						return SQLite3Constant.createBoolean(!equals);
					}
				}

			},
			// IN("IN"),
			LIKE("LIKE"), GLOB("GLOB");
			// MATCH("MATCH"),
			// REGEXP("REGEXP"),

			SQLite3Constant apply(SQLite3Constant left, SQLite3Constant right, CollateSequence collate) {
				return null;
			}

			private final String textRepresentation[];

			private BinaryComparisonOperator(String... textRepresentation) {
				this.textRepresentation = textRepresentation;
			}

			public static BinaryComparisonOperator getRandomOperator() {
				return Randomly.fromOptions(values());
			}

			public String getTextRepresentation() {
				return Randomly.fromOptions(textRepresentation);
			}

			public BinaryComparisonOperator reverse() {
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

			public SQLite3Constant applyOperand(SQLite3Constant left, TypeAffinity leftAffinity, SQLite3Constant right,
					TypeAffinity rightAffinity) {
				// If one operand has INTEGER, REAL or NUMERIC affinity and the other operand
				// has TEXT or BLOB or no affinity then NUMERIC affinity is applied to other
				// operand.
				if (leftAffinity.isNumeric() && (rightAffinity == TypeAffinity.TEXT
						|| rightAffinity == TypeAffinity.BLOB || rightAffinity == TypeAffinity.NONE)) {
					right = right.applyNumericAffinity();
				} else if (rightAffinity.isNumeric() && (leftAffinity == TypeAffinity.TEXT
						|| leftAffinity == TypeAffinity.BLOB || leftAffinity == TypeAffinity.NONE)) {
					left = left.applyNumericAffinity();
				}

				// If one operand has TEXT affinity and the other has no affinity, then TEXT
				// affinity is applied to the other operand.
				if (leftAffinity == TypeAffinity.TEXT && rightAffinity == TypeAffinity.NONE) {
					right = right.applyTextAffinity();
				} else if (rightAffinity == TypeAffinity.TEXT && leftAffinity == TypeAffinity.NONE) {
					left = left.applyTextAffinity();
				}
				// If either operand has an explicit collating function assignment using the
				// postfix COLLATE operator, then the explicit collating function is used for
				// comparison, with precedence to the collating function of the left operand.
				CollateSequence seq = left.getExplicitCollateSequence();
				if (seq == null) {
					seq = right.getExplicitCollateSequence();
				}
				// If either operand is a column, then the collating function of that column is
				// used with precedence to the left operand. For the purposes of the previous
				// sentence, a column name preceded by one or more unary "+" operators is still
				// considered a column name.
				if (seq == null) {
					seq = left.getImplicitCollateSequence();
				}
				if (seq == null) {
					seq = right.getImplicitCollateSequence();
				}
				// Otherwise, the BINARY collating function is used for comparison.
				if (seq == null) {
					seq = CollateSequence.BINARY;
				}
				return apply(left, right, seq);
			}

		}

		@Override
		public CollateSequence getExplicitCollateSequence() {
			if (left.getExplicitCollateSequence() != null) {
				return left.getExplicitCollateSequence();
			} else {
				return right.getExplicitCollateSequence();
			}
		}

	}

	public static class BinaryOperation extends SQLite3Expression {

		public enum BinaryOperator {
			CONCATENATE("||") {
				@Override
				public SQLite3Constant apply(SQLite3Constant left, SQLite3Constant right) {
					if (left.getExpectedValue() == null || right.getExpectedValue() == null) {
						return null;
					}
					if (left.getExpectedValue().isNull() || right.getExpectedValue().isNull()) {
						return SQLite3Constant.createNullConstant();
					}
					SQLite3Constant leftText = SQLite3Cast.castToText(left);
					SQLite3Constant rightText = SQLite3Cast.castToText(right);
					if (leftText == null || rightText == null) {
						return null;
					}
					return SQLite3Constant.createTextConstant(leftText.asString() + rightText.asString());
				}
			},
			MULTIPLY("*"), DIVIDE("/"), // division by zero results in zero
			REMAINDER("%"), PLUS("+") {
				@Override
				SQLite3Constant apply(SQLite3Constant left, SQLite3Constant right) {
					SQLite3Constant leftNumeric = SQLite3Cast.castToNumeric(left);
					SQLite3Constant rightNumeric = SQLite3Cast.castToNumeric(right);
					if (leftNumeric.isNull() || rightNumeric.isNull()) {
						return SQLite3Constant.createNullConstant();
					}
					if (leftNumeric.getDataType() == SQLite3DataType.INT) {
						long leftInt = leftNumeric.asInt();
						if (rightNumeric.getDataType() == SQLite3DataType.INT) {
							long rightInt = rightNumeric.asInt();
							try {
								long intResult = Math.addExact(leftInt, rightInt);
								return SQLite3Constant.createIntConstant(intResult);
							} catch (ArithmeticException e) {
								double realResult = (double) leftInt + (double) rightInt;
								return SQLite3Constant.createRealConstant(realResult);
							}
						} else {
							assert rightNumeric.getDataType() == SQLite3DataType.REAL;
							double rightDouble = rightNumeric.asDouble();
							return SQLite3Constant.createRealConstant(leftInt + rightDouble);
						}
					} else {
						assert leftNumeric.getDataType() == SQLite3DataType.REAL;
						double leftReal = leftNumeric.asDouble();
						if (rightNumeric.getDataType() == SQLite3DataType.INT) {
							long rightInt = rightNumeric.asInt();
							return SQLite3Constant.createRealConstant(leftReal + rightInt);
						} else {
							assert rightNumeric.getDataType() == SQLite3DataType.REAL;
							double rightReal = rightNumeric.asDouble();
							return SQLite3Constant.createRealConstant(leftReal + rightReal);
						}
					}
				}

			},
			MINUS("-") {
				@Override
				SQLite3Constant apply(SQLite3Constant left, SQLite3Constant right) {
					SQLite3Constant leftNumeric = SQLite3Cast.castToNumeric(left);
					SQLite3Constant rightNumeric = SQLite3Cast.castToNumeric(right);
					if (leftNumeric.isNull() || rightNumeric.isNull()) {
						return SQLite3Constant.createNullConstant();
					}
					if (leftNumeric.getDataType() == SQLite3DataType.INT) {
						long leftInt = leftNumeric.asInt();
						if (rightNumeric.getDataType() == SQLite3DataType.INT) {
							long rightInt = rightNumeric.asInt();
							try {
								long intResult = Math.subtractExact(leftInt, rightInt);
								return SQLite3Constant.createIntConstant(intResult);
							} catch (ArithmeticException e) {
								double realResult = (double) leftInt - (double) rightInt;
								return SQLite3Constant.createRealConstant(realResult);
							}
						} else {
							assert rightNumeric.getDataType() == SQLite3DataType.REAL;
							double rightDouble = rightNumeric.asDouble();
							return SQLite3Constant.createRealConstant(leftInt - rightDouble);
						}
					} else {
						assert leftNumeric.getDataType() == SQLite3DataType.REAL;
						double leftReal = leftNumeric.asDouble();
						if (rightNumeric.getDataType() == SQLite3DataType.INT) {
							long rightInt = rightNumeric.asInt();
							return SQLite3Constant.createRealConstant(leftReal - rightInt);
						} else {
							assert rightNumeric.getDataType() == SQLite3DataType.REAL;
							double rightReal = rightNumeric.asDouble();
							return SQLite3Constant.createRealConstant(leftReal - rightReal);
						}
					}
				}
			},
			SHIFT_LEFT("<<") {

				@Override
				SQLite3Constant apply(SQLite3Constant left, SQLite3Constant right) {
					return applyIntOperation(left, right, (leftResult, rightResult) -> {

						if (rightResult >= 0) {
							if (rightResult >= Long.SIZE) {
								return 0L;
							}
							return leftResult << rightResult;
						} else {
							return SHIFT_RIGHT.apply(left, SQLite3IntConstant.createIntConstant(-rightResult)).asInt();
						}

					});
				}

			},
			SHIFT_RIGHT(">>") {

				@Override
				SQLite3Constant apply(SQLite3Constant left, SQLite3Constant right) {
					return applyIntOperation(left, right, (leftResult, rightResult) -> {
						if (rightResult >= 0) {
							if (rightResult >= Long.SIZE) {
								return leftResult >= 0 ? 0L : -1L;
							}
							return leftResult >> rightResult;
						} else {
							if (rightResult == Long.MIN_VALUE) {
								return leftResult >= 0 ? 0 : -1L;
							}
							return SHIFT_LEFT.apply(left, SQLite3IntConstant.createIntConstant(-rightResult)).asInt();
						}

					});
				}

			},
			ARITHMETIC_AND("&") {
				@Override
				SQLite3Constant apply(SQLite3Constant left, SQLite3Constant right) {
					return applyIntOperation(left, right, (a, b) -> a & b);
				}

			},
			ARITHMETIC_OR("|") {
				@Override
				SQLite3Constant apply(SQLite3Constant left, SQLite3Constant right) {
					return applyIntOperation(left, right, (a, b) -> a | b);
				}

			},
			AND("AND") {

				@Override
				public SQLite3Constant apply(SQLite3Constant left, SQLite3Constant right) {

					if (left.getExpectedValue() == null || right.getExpectedValue() == null) {
						return null;
					} else {
						Optional<Boolean> leftBoolVal = SQLite3Cast.isTrue(left.getExpectedValue());
						Optional<Boolean> rightBoolVal = SQLite3Cast.isTrue(right.getExpectedValue());
						if (leftBoolVal.isPresent() && !leftBoolVal.get()) {
							return SQLite3Constant.createFalse();
						} else if (rightBoolVal.isPresent() && !rightBoolVal.get()) {
							return SQLite3Constant.createFalse();
						} else if (!rightBoolVal.isPresent() || !leftBoolVal.isPresent()) {
							return SQLite3Constant.createNullConstant();
						} else {
							return SQLite3Constant.createTrue();
						}
					}
				}

			},
			OR("OR") {
				@Override
				public SQLite3Constant apply(SQLite3Constant left, SQLite3Constant right) {
					if (left.getExpectedValue() == null || right.getExpectedValue() == null) {
						return null;
					} else {
						Optional<Boolean> leftBoolVal = SQLite3Cast.isTrue(left.getExpectedValue());
						Optional<Boolean> rightBoolVal = SQLite3Cast.isTrue(right.getExpectedValue());
						if (leftBoolVal.isPresent() && leftBoolVal.get()) {
							return SQLite3Constant.createTrue();
						} else if (rightBoolVal.isPresent() && rightBoolVal.get()) {
							return SQLite3Constant.createTrue();
						} else if (!rightBoolVal.isPresent() || !leftBoolVal.isPresent()) {
							return SQLite3Constant.createNullConstant();
						} else {
							return SQLite3Constant.createFalse();
						}
					}
				}
			};

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

			public SQLite3Constant applyOperand(SQLite3Constant left, TypeAffinity leftAffinity, SQLite3Constant right,
					TypeAffinity rightAffinity) {
				return apply(left, right);
			}

			public SQLite3Constant applyIntOperation(SQLite3Constant left, SQLite3Constant right,
					java.util.function.BinaryOperator<Long> func) {
				if (left.isNull() || right.isNull()) {
					return SQLite3Constant.createNullConstant();
				}
				SQLite3Constant leftInt = SQLite3Cast.castToInt(left);
				SQLite3Constant rightInt = SQLite3Cast.castToInt(right);
				long result = func.apply(leftInt.asInt(), rightInt.asInt());
				return SQLite3Constant.createIntConstant(result);
			}

			SQLite3Constant apply(SQLite3Constant left, SQLite3Constant right) {
				return null;
			}

		}

		@Override
		public CollateSequence getExplicitCollateSequence() {
			if (left.getExplicitCollateSequence() != null) {
				return left.getExplicitCollateSequence();
			} else {
				return right.getExplicitCollateSequence();
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

		@Override
		public SQLite3Constant getExpectedValue() {
			if (left.getExpectedValue() == null || right.getExpectedValue() == null) {
				return null;
			}
			return operation.applyOperand(left.getExpectedValue(), left.getAffinity(), right.getExpectedValue(),
					right.getAffinity());
		}

		public static BinaryOperation create(SQLite3Expression leftVal, SQLite3Expression rightVal, BinaryOperator op) {
			return new BinaryOperation(leftVal, rightVal, op);
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

		@Override
		public CollateSequence getExplicitCollateSequence() {
			if (left.getExplicitCollateSequence() != null) {
				return left.getExplicitCollateSequence();
			} else {
				return right.getExplicitCollateSequence();
			}
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

		/*
		 * When an expression is a simple reference to a column of a real table (not a
		 * VIEW or subquery) then the expression has the same affinity as the table
		 * column.
		 */
		@Override
		public TypeAffinity getAffinity() {
			switch (column.getColumnType()) {
			case BINARY:
				return TypeAffinity.BLOB;
			case INT:
				return TypeAffinity.INTEGER;
			case NONE:
				return TypeAffinity.NONE;
			case REAL:
				return TypeAffinity.REAL;
			case TEXT:
				return TypeAffinity.TEXT;
			default:
				throw new AssertionError(column);
			}
		}

		@Override
		public CollateSequence getExplicitCollateSequence() {
			return null;
		}

		@Override
		public CollateSequence getImplicitCollateSequence() {
			return column.getCollateSequence();
		}

	}

}
