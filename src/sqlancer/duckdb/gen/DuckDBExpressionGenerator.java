package sqlancer.duckdb.gen;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.ast.BinaryOperatorNode.Operator;
import sqlancer.ast.newast.ColumnReferenceNode;
import sqlancer.ast.newast.NewBetweenOperatorNode;
import sqlancer.ast.newast.NewBinaryOperatorNode;
import sqlancer.ast.newast.NewFunctionNode;
import sqlancer.ast.newast.NewUnaryPostfixOperatorNode;
import sqlancer.ast.newast.NewUnaryPrefixOperatorNode;
import sqlancer.ast.newast.Node;
import sqlancer.duckdb.DuckDBProvider.DuckDBGlobalState;
import sqlancer.duckdb.DuckDBSchema.DuckDBColumn;
import sqlancer.duckdb.DuckDBSchema.DuckDBCompositeDataType;
import sqlancer.duckdb.DuckDBSchema.DuckDBDataType;
import sqlancer.duckdb.ast.DuckDBConstant;
import sqlancer.duckdb.ast.DuckDBExpression;
import sqlancer.gen.UntypedExpressionGenerator;

public class DuckDBExpressionGenerator extends UntypedExpressionGenerator<Node<DuckDBExpression>, DuckDBColumn> {

	private final DuckDBGlobalState globalState;

	public DuckDBExpressionGenerator(DuckDBGlobalState globalState) {
		this.globalState = globalState;
	}

	private enum Expression {
		UNARY_POSTFIX, UNARY_PREFIX, BINARY_COMPARISON, BINARY_LOGICAL, BINARY_ARITHMETIC, CAST, FUNC, BETWEEN
	}

	protected Node<DuckDBExpression> generateExpression(int depth) {
		if (depth >= globalState.getOptions().getMaxExpressionDepth() || Randomly.getBoolean()) {
			return generateLeafNode();
		}
		Expression expr = Randomly.fromOptions(Expression.values());
		switch (expr) {
		case UNARY_PREFIX:
			return new NewUnaryPrefixOperatorNode<DuckDBExpression>(generateExpression(depth + 1),
					DuckDBUnaryPrefixOperator.getRandom());
		case UNARY_POSTFIX:
			return new NewUnaryPostfixOperatorNode<DuckDBExpression>(generateExpression(depth + 1),
					DuckDBUnaryPostfixOperator.getRandom());
		case BINARY_COMPARISON:
			Operator op = DuckDBBinaryComparisonOperator.getRandom();
			return new NewBinaryOperatorNode<DuckDBExpression>(generateExpression(depth + 1),
					generateExpression(depth + 1), op);
		case BINARY_LOGICAL:
			op = DuckDBBinaryLogicalOperator.getRandom();
			return new NewBinaryOperatorNode<DuckDBExpression>(generateExpression(depth + 1),
					generateExpression(depth + 1), op);
		case BINARY_ARITHMETIC:
			if (true) {
				throw new IgnoreMeException();
			}
			return new NewBinaryOperatorNode<DuckDBExpression>(generateExpression(depth + 1),
					generateExpression(depth + 1), DuckDBBinaryArithmeticOperator.getRandom());
		case CAST:
			return new DuckDBCastOperation(generateExpression(depth + 1), DuckDBCompositeDataType.getRandom());
		case FUNC:
			DBFunction func = DBFunction.getRandom();
			return new NewFunctionNode<DuckDBExpression, DBFunction>(generateExpressions(func.getNrArgs()), func);
		case BETWEEN:
			return new NewBetweenOperatorNode<DuckDBExpression>(generateExpression(depth + 1),
					generateExpression(depth + 1), generateExpression(depth + 1), Randomly.getBoolean());
		}
		return generateLeafNode();
	}

	@Override
	protected Node<DuckDBExpression> generateColumn() {
		DuckDBColumn column = Randomly.fromList(columns);
		return new ColumnReferenceNode<DuckDBExpression, DuckDBColumn>(column);
	}

	@Override
	public Node<DuckDBExpression> generateConstant() {
		if (Randomly.getBooleanWithRatherLowProbability()) {
			return DuckDBConstant.createNullConstant();
		}
		DuckDBDataType type = DuckDBDataType.getRandom();
		switch (type) {
		case INT:
			return DuckDBConstant.createIntConstant(globalState.getRandomly().getInteger(-30, 30)); // TODO:
																									// https://github.com/cwida/duckdb/issues/495
		case VARCHAR:
			return DuckDBConstant.createIntConstant(globalState.getRandomly().getInteger());
//			return DuckDBConstant.createStringConstant(globalState.getRandomly().getString());
		case BOOLEAN:
			return DuckDBConstant.createBooleanConstant(Randomly.getBoolean());
		case FLOAT:
			return DuckDBConstant.createFloatConstant(globalState.getRandomly().getDouble());
		default:
			throw new AssertionError();
		}
	}

	public class DuckDBCastOperation extends NewUnaryPostfixOperatorNode<DuckDBExpression> {

		public DuckDBCastOperation(Node<DuckDBExpression> expr, DuckDBCompositeDataType type) {
			super(expr, new Operator() {

				@Override
				public String getTextRepresentation() {
					return "::" + type.getPrimitiveDataType().toString();
				}
			});
		}

	}

	public enum DBFunction {
		ACOS(1), ASIN(1), ATAN(1), COS(1), SIN(1), TAN(1), COT(1), ATAN2(1), CEIL(1), CEILING(1), FLOOR(1), LOG(1),
		LOG10(1), LOG2(1), LN(1), PI(0), SQRT(1), POWER(1), CBRT(1), CONTAINS(2), PREFIX(2), SUFFIX(2), ABS(1),
		/*ROUND(2) https://github.com/cwida/duckdb/issues/521 ,*/ LENGTH(1), LOWER(1), UPPER(1), SUBSTRING(3), REVERSE(1), CONCAT(1, true), CONCAT_WS(1, true),
		INSTR(2), PRINTF(1, true);
//		REGEXP_MATCHES(2),
//		REGEX_REPLACE(3);

		private int nrArgs;
		private boolean isVariadic;

		DBFunction(int nrArgs) {
			this(nrArgs, false);
		}

		DBFunction(int nrArgs, boolean isVariadic) {
			this.nrArgs = nrArgs;
			this.isVariadic = isVariadic;
		}

		public static DBFunction getRandom() {
			return Randomly.fromOptions(values());
		}

		public int getNrArgs() {
			if (isVariadic) {
				return Randomly.smallNumber() + nrArgs;
			} else {
				return nrArgs;
			}
		}

	}

	public enum DuckDBUnaryPostfixOperator implements Operator {

		IS_NULL("IS NULL"), IS_NOT_NULL("IS NOT NULL");

		private String textRepr;

		private DuckDBUnaryPostfixOperator(String textRepr) {
			this.textRepr = textRepr;
		}

		@Override
		public String getTextRepresentation() {
			return textRepr;
		}

		public static DuckDBUnaryPostfixOperator getRandom() {
			return Randomly.fromOptions(values());
		}

	}

	public enum DuckDBUnaryPrefixOperator implements Operator {

		NOT("NOT"), PLUS("+"), MINUS("-");

		private String textRepr;

		private DuckDBUnaryPrefixOperator(String textRepr) {
			this.textRepr = textRepr;
		}

		@Override
		public String getTextRepresentation() {
			return textRepr;
		}

		public static DuckDBUnaryPrefixOperator getRandom() {
			return Randomly.fromOptions(values());
		}

	}

	public enum DuckDBBinaryLogicalOperator implements Operator {

		AND, OR;

		@Override
		public String getTextRepresentation() {
			return toString();
		}

		public static Operator getRandom() {
			return Randomly.fromOptions(values());
		}

	}

	public enum DuckDBBinaryArithmeticOperator implements Operator {
		ADD("+"), SUB("-"), MULT("*"), DIV("/"), MOD("%"), AND("&"), OR("|"), XOR("#"), LSHIFT("<<"), RSHIFT(">>");

		private String textRepr;

		private DuckDBBinaryArithmeticOperator(String textRepr) {
			this.textRepr = textRepr;
		}

		public static Operator getRandom() {
			return Randomly.fromOptions(values());
		}

		@Override
		public String getTextRepresentation() {
			return textRepr;
		}

	}

	public enum DuckDBBinaryComparisonOperator implements Operator {

		EQUALS("="), GREATER(">"), GREATER_EQUALS(">="), SMALLER("<"), SMALLER_EQUALS("<="), NOT_EQUALS("!="),
		LIKE("LIKE"), NOT_LIKE("NOT LIKE"), SIMILAR_TO("SIMILAR TO"), NOT_SIMILAR_TO("NOT SIMILAR TO");

		private String textRepr;

		private DuckDBBinaryComparisonOperator(String textRepr) {
			this.textRepr = textRepr;
		}

		public static Operator getRandom() {
			return Randomly.fromOptions(values());
		}

		@Override
		public String getTextRepresentation() {
			return textRepr;
		}

	}

}