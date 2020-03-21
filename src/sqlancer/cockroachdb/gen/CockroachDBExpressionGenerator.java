package sqlancer.cockroachdb.gen;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.cockroachdb.CockroachDBCommon;
import sqlancer.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBColumn;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBCompositeDataType;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBDataType;
import sqlancer.cockroachdb.ast.CockroachDBAggregate;
import sqlancer.cockroachdb.ast.CockroachDBAggregate.CockroachDBAggregateFunction;
import sqlancer.cockroachdb.ast.CockroachDBBetweenOperation;
import sqlancer.cockroachdb.ast.CockroachDBBetweenOperation.CockroachDBBetweenOperatorType;
import sqlancer.cockroachdb.ast.CockroachDBBinaryArithmeticOperation;
import sqlancer.cockroachdb.ast.CockroachDBBinaryArithmeticOperation.CockroachDBBinaryArithmeticOperator;
import sqlancer.cockroachdb.ast.CockroachDBBinaryComparisonOperator;
import sqlancer.cockroachdb.ast.CockroachDBBinaryComparisonOperator.CockroachDBComparisonOperator;
import sqlancer.cockroachdb.ast.CockroachDBBinaryLogicalOperation;
import sqlancer.cockroachdb.ast.CockroachDBBinaryLogicalOperation.CockroachDBBinaryLogicalOperator;
import sqlancer.cockroachdb.ast.CockroachDBCaseOperation;
import sqlancer.cockroachdb.ast.CockroachDBCast;
import sqlancer.cockroachdb.ast.CockroachDBCollate;
import sqlancer.cockroachdb.ast.CockroachDBColumnReference;
import sqlancer.cockroachdb.ast.CockroachDBConcatOperation;
import sqlancer.cockroachdb.ast.CockroachDBConstant;
import sqlancer.cockroachdb.ast.CockroachDBExpression;
import sqlancer.cockroachdb.ast.CockroachDBFunction;
import sqlancer.cockroachdb.ast.CockroachDBInOperation;
import sqlancer.cockroachdb.ast.CockroachDBMultiValuedComparison;
import sqlancer.cockroachdb.ast.CockroachDBMultiValuedComparison.MultiValuedComparisonOperator;
import sqlancer.cockroachdb.ast.CockroachDBMultiValuedComparison.MultiValuedComparisonType;
import sqlancer.cockroachdb.ast.CockroachDBNotOperation;
import sqlancer.cockroachdb.ast.CockroachDBOrderingTerm;
import sqlancer.cockroachdb.ast.CockroachDBRegexOperation;
import sqlancer.cockroachdb.ast.CockroachDBRegexOperation.CockroachDBRegexOperator;
import sqlancer.cockroachdb.ast.CockroachDBTypeAnnotation;
import sqlancer.cockroachdb.ast.CockroachDBUnaryPostfixOperation;
import sqlancer.cockroachdb.ast.CockroachDBUnaryPostfixOperation.CockroachDBUnaryPostfixOperator;

public class CockroachDBExpressionGenerator {

	private final CockroachDBGlobalState globalState;
	private List<CockroachDBColumn> columns = new ArrayList<>();

	public CockroachDBExpressionGenerator(CockroachDBGlobalState globalState) {
		this.globalState = globalState;
	}

	public CockroachDBExpressionGenerator setColumns(List<CockroachDBColumn> columns) {
		this.columns = columns;
		return this;
	}

	public CockroachDBExpression generateExpression(CockroachDBCompositeDataType dataType) {
		return generateExpression(dataType, 0);
	}
	
	public CockroachDBExpression generateAggregate() {
		CockroachDBAggregateFunction aggr = CockroachDBAggregateFunction.getRandom();
		return generateWindowFunction(aggr);
	}

	public CockroachDBAggregate generateWindowFunction(CockroachDBAggregateFunction aggr) throws AssertionError {
		CockroachDBCompositeDataType type;
		switch (aggr) {
		case SQRDIFF:
		case STDDEV:
		case SUM_INT:
		case VARIANCE:
		case BIT_AND:
		case BIT_OR:
			type = CockroachDBDataType.INT.get();
			break;
		case XOR_AGG:
			type = Randomly.fromOptions(CockroachDBDataType.INT, CockroachDBDataType.BYTES).get();
			break;
		case BOOL_AND:
		case BOOL_OR:
			type = CockroachDBDataType.BOOL.get();
			break;
		case MIN:
		case MAX:
		case COUNT:
			type = getRandomType();
			break;
		case COUNT_ROWS:
			return new CockroachDBAggregate(aggr, Collections.emptyList());
		case SUM:
		case AVG:
			type = Randomly.fromOptions(CockroachDBDataType.INT, CockroachDBDataType.FLOAT, CockroachDBDataType.DECIMAL).get();
			break;
		default:
			throw new AssertionError();
		}
		return new CockroachDBAggregate(aggr, Arrays.asList(generateExpression(type)));
	}

	public List<CockroachDBExpression> generateExpressions(int nr) {
		List<CockroachDBExpression> expressions = new ArrayList<>();
		for (int i = 0; i < nr; i++) {
			expressions.add(generateExpression(getRandomType()));
		}
		return expressions;
	}

	public List<CockroachDBExpression> getOrderingTerms() {
		List<CockroachDBExpression> orderingTerms = new ArrayList<>();
		int nr = 1;
		while (Randomly.getBooleanWithSmallProbability()) {
			nr++;
		}
		for (int i = 0; i < nr; i++) {
			CockroachDBExpression expr = generateExpression(getRandomType());
			if (Randomly.getBoolean()) {
				expr = new CockroachDBOrderingTerm(expr, Randomly.getBoolean());
			}
			orderingTerms.add(expr);
		}
		return orderingTerms;
	}

	public CockroachDBExpression generateExpression(CockroachDBCompositeDataType type, int depth) {
//		if (type == CockroachDBDataType.FLOAT && Randomly.getBooleanWithRatherLowProbability()) {
//			type = CockroachDBDataType.INT;
//		}
		if (depth >= globalState.getOptions().getMaxExpressionDepth()
				|| Randomly.getBoolean()) {
			return generateLeafNode(type);
		} else {
			if (Randomly.getBoolean()) {
				List<CockroachDBFunction> applicableFunctions = CockroachDBFunction.getFunctionsCompatibleWith(type);
				if (!applicableFunctions.isEmpty()) {
					CockroachDBFunction function = Randomly.fromList(applicableFunctions);
					return function.getCall(type, this, depth + 1);
				}
			}
			if (Randomly.getBooleanWithRatherLowProbability()) {
				if (Randomly.getBoolean()) {
					return new CockroachDBCast(generateExpression(getRandomType(), depth + 1), type);
				} else {
					return new CockroachDBTypeAnnotation(generateExpression(type, depth + 1), type);
				}
			}
			if (Randomly.getBooleanWithRatherLowProbability()) {
				List<CockroachDBExpression> conditions = new ArrayList<>();
				List<CockroachDBExpression> cases = new ArrayList<>();
				for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
					conditions.add(generateExpression(CockroachDBDataType.BOOL.get(), depth + 1));
					cases.add(generateExpression(type, depth + 1));
				}
				CockroachDBExpression elseExpr = null;
				if (Randomly.getBoolean()) {
					elseExpr = generateExpression(type, depth + 1);
				}
				return new CockroachDBCaseOperation(conditions, cases, elseExpr);

			}

			switch (type.getPrimitiveDataType()) {
			case BOOL:
				return generateBooleanExpression(depth);
			case INT:
			case SERIAL:
				return new CockroachDBBinaryArithmeticOperation(generateExpression(CockroachDBDataType.INT.get(), depth + 1), generateExpression(CockroachDBDataType.INT.get(), depth + 1), CockroachDBBinaryArithmeticOperator.getRandom());
 // new CockroachDBUnaryArithmeticOperation(generateExpression(type, depth
												// + 1),
			// CockroachDBUnaryAritmeticOperator.getRandom()); /* wait for
			// https://github.com/cockroachdb/cockroach/issues/44137 */
			case STRING:
			case BYTES: // TODO split
				CockroachDBExpression stringExpr = generateStringExpression(depth);
				if (Randomly.getBoolean()) {
					stringExpr = new CockroachDBCollate(stringExpr, CockroachDBCommon.getRandomCollate());
				}
				return stringExpr; // TODO
			case FLOAT:
			case VARBIT:
			case BIT:
			case INTERVAL:
			case TIMESTAMP:
			case DECIMAL:
			case TIMESTAMPTZ:
			case JSONB:
			case TIME:
			case TIMETZ:
				return generateLeafNode(type); // TODO
			default:
				throw new AssertionError(type);
			}
		}
	}

	private CockroachDBExpression generateLeafNode(CockroachDBCompositeDataType type) {
		if (Randomly.getBooleanWithRatherLowProbability() || !canGenerateConstantOfType(type)) {
			return generateConstant(type);
		} else {
			return getRandomColumn(type);
		}
	}

	private enum BooleanExpression {
		NOT, COMPARISON, AND_OR_CHAIN, REGEX, IS_NULL, IS_NAN, IN, BETWEEN, MULTI_VALUED_COMPARISON
	}

	private enum StringExpression {
		CONCAT
	}
	
	private CockroachDBExpression generateStringExpression(int depth) {
		StringExpression exprType = Randomly.fromOptions(StringExpression.values());
		switch (exprType) {
		case CONCAT:
			return new CockroachDBConcatOperation(generateExpression(CockroachDBDataType.STRING.get(), depth + 1), generateExpression(CockroachDBDataType.STRING.get(), depth + 1));
		default:
			throw new AssertionError(exprType);
		}
	}

	private CockroachDBExpression generateBooleanExpression(int depth) {
		BooleanExpression exprType = Randomly.fromOptions(BooleanExpression.values());
		CockroachDBExpression expr;
		switch (exprType) {
		case NOT:
			return new CockroachDBNotOperation(generateExpression(CockroachDBDataType.BOOL.get(), depth + 1));
		case COMPARISON:
			return getBinaryComparison(depth);
		case AND_OR_CHAIN:
			return getAndOrChain(depth);
		case REGEX:
			return new CockroachDBRegexOperation(generateExpression(CockroachDBDataType.STRING.get(), depth + 1),
					generateExpression(CockroachDBDataType.STRING.get(), depth + 1),
					CockroachDBRegexOperator.getRandom());
		case IS_NULL:
			return new CockroachDBUnaryPostfixOperation(generateExpression(getRandomType(), depth + 1), Randomly
					.fromOptions(CockroachDBUnaryPostfixOperator.IS_NULL, CockroachDBUnaryPostfixOperator.IS_NOT_NULL));
		case IS_NAN:
			return new CockroachDBUnaryPostfixOperation(generateExpression(CockroachDBDataType.FLOAT.get(), depth + 1),
					Randomly.fromOptions(CockroachDBUnaryPostfixOperator.IS_NAN,
							CockroachDBUnaryPostfixOperator.IS_NOT_NAN));
		case IN:
			return getInOperation(depth);
		case BETWEEN:
			CockroachDBCompositeDataType type = getRandomType();
			expr = generateExpression(type, depth + 1);
			CockroachDBExpression left = generateExpression(type, depth + 1);
			CockroachDBExpression right = generateExpression(type, depth + 1);
			return new CockroachDBBetweenOperation(expr, left, right, CockroachDBBetweenOperatorType.getRandom());
		case MULTI_VALUED_COMPARISON: // TODO other operators
			type = getRandomType();
			left = generateExpression(type, depth + 1);
			List<CockroachDBExpression> rightList;
			do {
				rightList = generateExpressions(type, depth + 1);
			} while (rightList.size() <= 1);
			return new CockroachDBMultiValuedComparison(left, rightList, MultiValuedComparisonType.getRandom(), MultiValuedComparisonOperator.getRandomGenericComparisonOperator());
		default:
			throw new AssertionError(exprType);
		}
	}

	private CockroachDBExpression getAndOrChain(int depth) {
		CockroachDBExpression left = generateExpression(CockroachDBDataType.BOOL.get(), depth + 1);
		for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
			CockroachDBExpression right = generateExpression(CockroachDBDataType.BOOL.get(), depth + 1);
			left = new CockroachDBBinaryLogicalOperation(left, right, CockroachDBBinaryLogicalOperator.getRandom());
		}
		return left;
	}

	private CockroachDBExpression getInOperation(int depth) {
		CockroachDBCompositeDataType type = getRandomType();
		return new CockroachDBInOperation(generateExpression(type, depth + 1), generateExpressions(type, depth + 1));
	}

	private CockroachDBCompositeDataType getRandomType() {
		if (columns.isEmpty() || Randomly.getBoolean()) {
			return CockroachDBCompositeDataType.getRandom();
		} else {
			return Randomly.fromList(columns).getColumnType();
		}
	}

	private List<CockroachDBExpression> generateExpressions(CockroachDBCompositeDataType type, int depth) {
		List<CockroachDBExpression> expressions = new ArrayList<>();
		for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
			expressions.add(generateExpression(type, depth + 1));
		}
		return expressions;
	}

	private CockroachDBExpression getBinaryComparison(int depth) {
		CockroachDBCompositeDataType type = getRandomType();
		CockroachDBExpression left = generateExpression(type, depth + 1);
		CockroachDBExpression right = generateExpression(type, depth + 1);
		return new CockroachDBBinaryComparisonOperator(left, right, CockroachDBComparisonOperator.getRandom());
	}

	private boolean canGenerateConstantOfType(CockroachDBCompositeDataType type) {
		return columns.stream().anyMatch(c -> c.getColumnType() == type);
	}

	private CockroachDBExpression getRandomColumn(CockroachDBCompositeDataType type) {
		CockroachDBColumn column = Randomly
				.fromList(columns.stream().filter(c -> c.getColumnType() == type).collect(Collectors.toList()));
		CockroachDBExpression columnReference = new CockroachDBColumnReference(column);
		if (column.getColumnType().isString() && Randomly.getBooleanWithRatherLowProbability()) {
			columnReference = new CockroachDBCollate(columnReference, CockroachDBCommon.getRandomCollate());
		}
		return columnReference;
	}

	public CockroachDBExpression generateConstant(CockroachDBCompositeDataType type) {
		if (Randomly.getBooleanWithRatherLowProbability()) {
			return CockroachDBConstant.createNullConstant();
		}
		switch (type.getPrimitiveDataType()) {
		case INT:
		case SERIAL:
			return CockroachDBConstant.createIntConstant(globalState.getRandomly().getInteger());
		case BOOL:
			return CockroachDBConstant.createBooleanConstant(Randomly.getBoolean());
		case STRING:
		case BYTES: // TODO: also generate byte constants
			CockroachDBExpression strConst = getStringConstant();
			return strConst;
		case FLOAT:
			return CockroachDBConstant.createFloatConstant(globalState.getRandomly().getDouble());
		case BIT:
		case VARBIT:
			return CockroachDBConstant.createBitConstant(globalState.getRandomly().getInteger());
		case INTERVAL:
			return CockroachDBConstant.createIntervalConstant(globalState.getRandomly().getInteger(), globalState.getRandomly().getInteger(), globalState.getRandomly().getInteger(), globalState.getRandomly().getInteger(), globalState.getRandomly().getInteger(), globalState.getRandomly().getInteger());
		case TIMESTAMP:
			return CockroachDBConstant.createTimestampConstant(globalState.getRandomly().getInteger());
		case TIME:
			return CockroachDBConstant.createTimeConstant(globalState.getRandomly().getInteger());
		case DECIMAL:
		case TIMESTAMPTZ:
		case JSONB:
		case TIMETZ:
			return CockroachDBConstant.createNullConstant(); // TODO
		default:
			throw new AssertionError(type);
		}
	}

	private CockroachDBExpression getStringConstant() {
		CockroachDBExpression strConst = CockroachDBConstant
				.createStringConstant(globalState.getRandomly().getString());
		if (Randomly.getBooleanWithRatherLowProbability()) {
			strConst = new CockroachDBCollate(strConst, CockroachDBCommon.getRandomCollate());
		}
		return strConst;
	}

	public CockroachDBGlobalState getGlobalState() {
		return globalState;
	}


}
