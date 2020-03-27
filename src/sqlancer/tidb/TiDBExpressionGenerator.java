package sqlancer.tidb;

import java.util.ArrayList;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.tidb.TiDBProvider.TiDBGlobalState;
import sqlancer.tidb.TiDBSchema.TiDBColumn;
import sqlancer.tidb.TiDBSchema.TiDBDataType;
import sqlancer.tidb.ast.TiDBBinaryComparisonOperation;
import sqlancer.tidb.ast.TiDBBinaryComparisonOperation.TiDBComparisonOperator;
import sqlancer.tidb.ast.TiDBCollate;
import sqlancer.tidb.ast.TiDBColumnReference;
import sqlancer.tidb.ast.TiDBConstant;
import sqlancer.tidb.ast.TiDBExpression;
import sqlancer.tidb.ast.TiDBFunctionCall;
import sqlancer.tidb.ast.TiDBFunctionCall.TiDBFunction;
import sqlancer.tidb.ast.TiDBRegexOperation;
import sqlancer.tidb.ast.TiDBRegexOperation.TiDBRegexOperator;
import sqlancer.tidb.ast.TiDBUnaryPostfixOperation;
import sqlancer.tidb.ast.TiDBUnaryPostfixOperation.TiDBUnaryPostfixOperator;

public class TiDBExpressionGenerator {

	private final TiDBGlobalState globalState;
	private List<TiDBColumn> columns = new ArrayList<>();

	public TiDBExpressionGenerator(TiDBGlobalState globalState) {
		this.globalState = globalState;
	}

	private static enum Gen {
//		UNARY_PREFIX, //
		UNARY_POSTFIX, //
		CONSTANT, //
		COLUMN, //
		COMPARISON,
		REGEX,
		COLLATE,
		FUNCTION,
//		BINARY_ARITHMETIC
	}
	
	public TiDBExpression generateExpression() {
		return generateExpression(0);
	}

	public List<TiDBExpression> generateOrderBys() {
		List<TiDBExpression> list = new ArrayList<>();
		do {
			list.add(generateExpression(0));
		} while (Randomly.getBoolean());
		return list;
	}
	
	private TiDBExpression generateExpression(int depth) {
		if (depth >= globalState.getOptions().getMaxExpressionDepth() || Randomly.getBoolean()) {
			return generateLeafNode();
		}
		switch (Randomly.fromOptions(Gen.values())) {
		case UNARY_POSTFIX:
			return new TiDBUnaryPostfixOperation(generateExpression(depth + 1), TiDBUnaryPostfixOperator.getRandom());
// https://github.com/pingcap/tidb/issues/15725
//		case UNARY_PREFIX:
//			return new TiDBUnaryPrefixOperation(generateExpression(depth + 1), TiDBUnaryPrefixOperator.getRandom());
		case COLUMN:
			return generateColumn();
		case CONSTANT:
			return generateConstant();
		case COMPARISON:
			return new TiDBBinaryComparisonOperation(generateExpression(depth + 1), generateExpression(depth + 1), TiDBComparisonOperator.getRandom());
		case REGEX:
			return new TiDBRegexOperation(generateExpression(depth + 1), generateExpression(depth + 1), TiDBRegexOperator.getRandom());
		case COLLATE:
			return new TiDBCollate(generateExpression(depth + 1), Randomly.fromOptions("utf8mb4_bin", "latin1_bin", "binary", "ascii_bin", "utf8_bin"));
		case FUNCTION:
			TiDBFunction func = TiDBFunction.getRandom();
			return new TiDBFunctionCall(func, generateExpressions(depth, func.getNrArgs()));
//		case BINARY_ARITHMETIC:
//			return new TiDBBinaryArithmeticOperation(generateExpression(depth + 1), generateExpression(depth + 1), TiDBBinaryArithmeticOperator.getRandom());
		default:
			throw new AssertionError();
		}
	}

	private List<TiDBExpression> generateExpressions(int depth, int nrArgs) {
		List<TiDBExpression> args = new ArrayList<>();
		for (int i = 0; i < nrArgs; i++) {
			args.add(generateExpression(depth + 1));
		}
		return args;
	}

	private TiDBExpression generateLeafNode() {
		if (Randomly.getBoolean() || columns.isEmpty()) {
			return generateConstant();
		} else {
			return generateColumn();
		}
	}

	private TiDBExpression generateColumn() {
		TiDBColumn column = Randomly.fromList(columns);
		return new TiDBColumnReference(column);

	}

	public TiDBExpression generateConstant() {
		TiDBDataType type = TiDBDataType.getRandom();
		if (Randomly.getBooleanWithRatherLowProbability()) {
			return TiDBConstant.createNullConstant();
		}
		switch (type) {
		case INT:
			return TiDBConstant.createIntConstant(globalState.getRandomly().getInteger());
		case TEXT: // TODO: wait for https://github.com/pingcap/tidb/issues/15743
			return TiDBConstant.createIntConstant(globalState.getRandomly().getInteger());
//			return TiDBConstant.createStringConstant(globalState.getRandomly().getString());
		case BOOL:
			return TiDBConstant.createBooleanConstant(Randomly.getBoolean());
		case DOUBLE:
		case FLOAT: // TODO: wait for https://github.com/pingcap/tidb/issues/15743
			return TiDBConstant.createIntConstant(globalState.getRandomly().getInteger());
//			return TiDBConstant.createFloatConstant(globalState.getRandomly().getDouble());
		default:
			throw new AssertionError();
		}
	}

	public TiDBExpressionGenerator setColumns(List<TiDBColumn> columns) {
		this.columns = columns;
		return this;
	}
}
