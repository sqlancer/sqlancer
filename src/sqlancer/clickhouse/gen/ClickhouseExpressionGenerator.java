package sqlancer.clickhouse.gen;

import sqlancer.Randomly;
import sqlancer.ast.BinaryOperatorNode.Operator;
import sqlancer.ast.newast.ColumnReferenceNode;
import sqlancer.ast.newast.NewBinaryOperatorNode;
import sqlancer.ast.newast.NewUnaryPostfixOperatorNode;
import sqlancer.ast.newast.NewUnaryPrefixOperatorNode;
import sqlancer.ast.newast.Node;
import sqlancer.clickhouse.ClickhouseProvider.ClickhouseGlobalState;
import sqlancer.clickhouse.ClickhouseSchema.ClickhouseColumn;
import sqlancer.clickhouse.ClickhouseSchema.ClickhouseDataType;
import sqlancer.clickhouse.ast.ClickhouseConstant;
import sqlancer.clickhouse.ast.ClickhouseExpression;
import sqlancer.gen.UntypedExpressionGenerator;

public class ClickhouseExpressionGenerator
		extends UntypedExpressionGenerator<Node<ClickhouseExpression>, ClickhouseColumn> {

	private final ClickhouseGlobalState globalState;

	public ClickhouseExpressionGenerator(ClickhouseGlobalState globalState) {
		this.globalState = globalState;
	}

	private enum Expression {
		UNARY_POSTFIX, UNARY_PREFIX, BINARY_COMPARISON, BINARY_LOGICAL
	}

	protected Node<ClickhouseExpression> generateExpression(int depth) {
		if (depth >= globalState.getOptions().getMaxExpressionDepth() || Randomly.getBoolean()) {
			return generateLeafNode();
		}
		Expression expr = Randomly.fromOptions(Expression.values());
		switch (expr) {
		case UNARY_PREFIX:
			return new NewUnaryPrefixOperatorNode<ClickhouseExpression>(generateExpression(depth + 1),
					ClickhouseUnaryPrefixOperator.getRandom());
		case UNARY_POSTFIX:
			return new NewUnaryPostfixOperatorNode<ClickhouseExpression>(generateExpression(depth + 1),
					ClickhouseUnaryPostfixOperator.getRandom());
		case BINARY_COMPARISON:
			return new NewBinaryOperatorNode<ClickhouseExpression>(generateExpression(depth + 1),
					generateExpression(depth + 1), ClickhouseBinaryComparisonOperator.getRandom());
		case BINARY_LOGICAL:
			return new NewBinaryOperatorNode<ClickhouseExpression>(generateExpression(depth + 1),
					generateExpression(depth + 1), ClickhouseBinaryLogicalOperator.getRandom());
		}
		return generateLeafNode();
	}

	@Override
	protected Node<ClickhouseExpression> generateColumn() {
		ClickhouseColumn column = Randomly.fromList(columns);
		return new ColumnReferenceNode<ClickhouseExpression, ClickhouseColumn>(column);
	}

	@Override
	public Node<ClickhouseExpression> generateConstant() {
		ClickhouseDataType type = ClickhouseDataType.getRandom();
		if (Randomly.getBooleanWithRatherLowProbability()) {
			return ClickhouseConstant.createNullConstant();
		}
		switch (type) {
		case INT:
			return ClickhouseConstant.createIntConstant(globalState.getRandomly().getInteger());
		case STRING:
			return ClickhouseConstant.createStringConstant(globalState.getRandomly().getString());
		default:
			throw new AssertionError();
		}
	}

	public enum ClickhouseUnaryPostfixOperator implements Operator {

		IS_NULL("IS NULL"), IS_NOT_NULL("IS NOT NULL");

		private String textRepr;

		private ClickhouseUnaryPostfixOperator(String textRepr) {
			this.textRepr = textRepr;
		}

		@Override
		public String getTextRepresentation() {
			return textRepr;
		}

		public static ClickhouseUnaryPostfixOperator getRandom() {
			return Randomly.fromOptions(values());
		}

	}

	public enum ClickhouseUnaryPrefixOperator implements Operator {

		NOT("NOT");

		private String textRepr;

		private ClickhouseUnaryPrefixOperator(String textRepr) {
			this.textRepr = textRepr;
		}

		@Override
		public String getTextRepresentation() {
			return textRepr;
		}

		public static ClickhouseUnaryPrefixOperator getRandom() {
			return Randomly.fromOptions(values());
		}

	}

	public enum ClickhouseBinaryLogicalOperator implements Operator {

		AND, OR;

		@Override
		public String getTextRepresentation() {
			return toString();
		}

		public static Operator getRandom() {
			return Randomly.fromOptions(values());
		}

	}

	public enum ClickhouseBinaryComparisonOperator implements Operator {

		EQUALS("="), GREATER(">"), GREATER_EQUALS(">="), SMALLER("<"), SMALLER_EQUALS("<="), NOT_EQUALS("!="),
		LIKE("LIKE"), NOT_LIKE("NOT LIKE");

		private String textRepr;

		private ClickhouseBinaryComparisonOperator(String textRepr) {
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