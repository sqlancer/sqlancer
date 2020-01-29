package lama.cockroachdb;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import lama.Randomly;
import lama.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;
import lama.cockroachdb.CockroachDBSchema.CockroachDBColumn;
import lama.cockroachdb.CockroachDBSchema.CockroachDBCompositeDataType;
import lama.cockroachdb.CockroachDBSchema.CockroachDBDataType;
import lama.cockroachdb.ast.CockroachDBBetweenOperation;
import lama.cockroachdb.ast.CockroachDBBetweenOperation.CockroachDBBetweenOperatorType;
import lama.cockroachdb.ast.CockroachDBBinaryComparisonOperator;
import lama.cockroachdb.ast.CockroachDBBinaryComparisonOperator.CockroachDBComparisonOperator;
import lama.cockroachdb.ast.CockroachDBBinaryLogicalOperation;
import lama.cockroachdb.ast.CockroachDBBinaryLogicalOperation.CockroachDBBinaryLogicalOperator;
import lama.cockroachdb.ast.CockroachDBCaseOperation;
import lama.cockroachdb.ast.CockroachDBCast;
import lama.cockroachdb.ast.CockroachDBCollate;
import lama.cockroachdb.ast.CockroachDBColumnReference;
import lama.cockroachdb.ast.CockroachDBConstant;
import lama.cockroachdb.ast.CockroachDBExpression;
import lama.cockroachdb.ast.CockroachDBInOperation;
import lama.cockroachdb.ast.CockroachDBNotOperation;
import lama.cockroachdb.ast.CockroachDBOrderingTerm;
import lama.cockroachdb.ast.CockroachDBRegexOperation;
import lama.cockroachdb.ast.CockroachDBRegexOperation.CockroachDBRegexOperator;
import lama.cockroachdb.ast.CockroachDBTypeAnnotation;
import lama.cockroachdb.ast.CockroachDBUnaryPostfixOperation;
import lama.cockroachdb.ast.CockroachDBUnaryPostfixOperation.CockroachDBUnaryPostfixOperator;

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

	public List<CockroachDBExpression> generateExpressions(int nr) {
		List<CockroachDBExpression> expressions = new ArrayList<>();
		for (int i = 0; i < nr; i++) {
			expressions.add(generateExpression(getRandomType()));
		}
		return expressions;
	}

	public List<CockroachDBExpression> getOrderingTerms() {
		List<CockroachDBExpression> orderingTerms = new ArrayList<>();
		for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
			CockroachDBExpression expr = generateExpression(getRandomType());
			if (Randomly.getBoolean()) {
				expr = new CockroachDBOrderingTerm(expr, Randomly.getBoolean());
			}
			orderingTerms.add(expr);
		}
		return orderingTerms;
	}

	CockroachDBExpression generateExpression(CockroachDBCompositeDataType type, int depth) {
//		if (type == CockroachDBDataType.FLOAT && Randomly.getBooleanWithRatherLowProbability()) {
//			type = CockroachDBDataType.INT;
//		}
		if (depth >= globalState.getOptions().getMaxExpressionDepth()
				|| Randomly.getBooleanWithRatherLowProbability()) {
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
				return generateLeafNode(type);
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

	private enum StringExpression {
		REGEX
	}

	private enum BooleanExpression {
		NOT, COMPARISON, AND_OR_CHAIN, REGEX, IS_NULL, IS_NAN, IN, BETWEEN
	}

	private CockroachDBExpression generateStringExpression(int depth) {
		StringExpression exprType = Randomly.fromOptions(StringExpression.values());
		return generateConstant(CockroachDBDataType.STRING.get());
//		switch (exprType) {
//		default:
//			throw new AssertionError(exprType);
//		}
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
		if (columns.isEmpty() || Randomly.getBooleanWithSmallProbability()) {
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

}
