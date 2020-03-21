package sqlancer.postgres.gen;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.postgres.PostgresCompoundDataType;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresProvider;
import sqlancer.postgres.PostgresSchema.PostgresColumn;
import sqlancer.postgres.PostgresSchema.PostgresDataType;
import sqlancer.postgres.PostgresSchema.PostgresRowValue;
import sqlancer.postgres.ast.PostgresBetweenOperation;
import sqlancer.postgres.ast.PostgresBinaryArithmeticOperation;
import sqlancer.postgres.ast.PostgresBinaryArithmeticOperation.PostgresBinaryOperator;
import sqlancer.postgres.ast.PostgresBinaryBitOperation;
import sqlancer.postgres.ast.PostgresBinaryBitOperation.PostgresBinaryBitOperator;
import sqlancer.postgres.ast.PostgresBinaryComparisonOperation;
import sqlancer.postgres.ast.PostgresBinaryLogicalOperation;
import sqlancer.postgres.ast.PostgresBinaryLogicalOperation.BinaryLogicalOperator;
import sqlancer.postgres.ast.PostgresBinaryRangeOperation;
import sqlancer.postgres.ast.PostgresBinaryRangeOperation.PostgresBinaryRangeComparisonOperator;
import sqlancer.postgres.ast.PostgresBinaryRangeOperation.PostgresBinaryRangeOperator;
import sqlancer.postgres.ast.PostgresCastOperation;
import sqlancer.postgres.ast.PostgresCollate;
import sqlancer.postgres.ast.PostgresColumnValue;
import sqlancer.postgres.ast.PostgresConcatOperation;
import sqlancer.postgres.ast.PostgresConstant;
import sqlancer.postgres.ast.PostgresExpression;
import sqlancer.postgres.ast.PostgresFunction;
import sqlancer.postgres.ast.PostgresFunction.PostgresFunctionWithResult;
import sqlancer.postgres.ast.PostgresFunctionWithUnknownResult;
import sqlancer.postgres.ast.PostgresInOperation;
import sqlancer.postgres.ast.PostgresLikeOperation;
import sqlancer.postgres.ast.PostgresOrderByTerm;
import sqlancer.postgres.ast.PostgresOrderByTerm.ForClause;
import sqlancer.postgres.ast.PostgresOrderByTerm.PostgresOrder;
import sqlancer.postgres.ast.PostgresPOSIXRegularExpression;
import sqlancer.postgres.ast.PostgresPOSIXRegularExpression.POSIXRegex;
import sqlancer.postgres.ast.PostgresPostfixOperation;
import sqlancer.postgres.ast.PostgresPostfixOperation.PostfixOperator;
import sqlancer.postgres.ast.PostgresPrefixOperation;
import sqlancer.postgres.ast.PostgresPrefixOperation.PrefixOperator;
import sqlancer.postgres.ast.PostgresSimilarTo;

public class PostgresExpressionGenerator {

	private final int MAX_DEPTH = 3;

	private Randomly r;

	private List<PostgresColumn> columns;

	private PostgresRowValue rw;

	private boolean expectedResult;

	private PostgresGlobalState globalState;

	public PostgresExpressionGenerator(Randomly r) {
		this.r = r;
	}

	public PostgresExpressionGenerator setColumns(List<PostgresColumn> columns) {
		this.columns = columns;
		return this;
	}

	public PostgresExpressionGenerator setRowValue(PostgresRowValue rw) {
		this.rw = rw;
		return this;
	}

	public PostgresExpressionGenerator expectedResult() {
		this.expectedResult = true;
		return this;
	}

	public static PostgresExpression generateExpression(Randomly r) {
		return new PostgresExpressionGenerator(r).generateExpression(0);
	}

	PostgresExpression generateExpression(int depth) {
		return generateExpression(depth, PostgresDataType.getRandomType());
	}
	
	public List<PostgresExpression> generateOrderBy() {
		List<PostgresExpression> orderBys = new ArrayList<>();
		for (int i = 0; i < Randomly.smallNumber(); i++) {
			ForClause forClause = null;
			if (Randomly.getBoolean()) {
				forClause = ForClause.getRandom();
			}
			orderBys.add(new PostgresOrderByTerm(PostgresColumnValue.create(Randomly.fromList(columns), null),
					PostgresOrder.getRandomOrder(), forClause));
		}
		return orderBys;
	}

	private enum BooleanExpression {
		CONSTANT, POSTFIX_OPERATOR, COLUMN, NOT, BINARY_LOGICAL_OPERATOR, BINARY_COMPARISON, FUNCTION, CAST, LIKE,
		BETWEEN, IN_OPERATION, SIMILAR_TO, POSIX_REGEX, BINARY_RANGE_COMPARISON;
	}
	
	private PostgresExpression generateFunctionWithUnknownResult(int depth, PostgresDataType type) {
		List<PostgresFunctionWithUnknownResult> supportedFunctions = PostgresFunctionWithUnknownResult.getSupportedFunctions(type);
		if (supportedFunctions.isEmpty()) {
			throw new IgnoreMeException();
		}
		PostgresFunctionWithUnknownResult randomFunction = Randomly.fromList(supportedFunctions);
		return new PostgresFunction(randomFunction, type, randomFunction.getArguments(type, this));
	}

	private PostgresExpression generateFunctionWithKnownResult(int depth, PostgresDataType type) {
		List<PostgresFunctionWithResult> functions = Stream.of(PostgresFunction.PostgresFunctionWithResult.values())
				.filter(f -> f.supportsReturnType(type)).collect(Collectors.toList());
		if (functions.isEmpty()) {
			throw new IgnoreMeException();
		}
		PostgresFunctionWithResult randomFunction = Randomly.fromList(functions);
		int nrArgs = randomFunction.getNrArgs();
		if (randomFunction.isVariadic()) {
			nrArgs += Randomly.smallNumber();
		}
		PostgresDataType[] argTypes = randomFunction.getInputTypesForReturnType(type, nrArgs);
		PostgresExpression[] args = new PostgresExpression[nrArgs];
		do {
			for (int i = 0; i < args.length; i++) {
				args[i] = generateExpression(depth + 1, argTypes[i]);
			}
		} while (!randomFunction.checkArguments(args));
		PostgresFunction f = new PostgresFunction(randomFunction, type, args);
		return f;
	}

	private PostgresExpression generateBooleanExpression(int depth) {
		BooleanExpression option;
		if (depth >= MAX_DEPTH) {
			if (Randomly.getBooleanWithSmallProbability()) {
				option = BooleanExpression.CONSTANT;
			} else {
				option = BooleanExpression.COLUMN;
			}
		} else {
			List<BooleanExpression> validOptions = new ArrayList<>(Arrays.asList(BooleanExpression.values()));
			if (PostgresProvider.GENERATE_ONLY_KNOWN) {
				validOptions.remove(BooleanExpression.SIMILAR_TO);
				validOptions.remove(BooleanExpression.POSIX_REGEX);
				validOptions.remove(BooleanExpression.BINARY_RANGE_COMPARISON);
			}
			option = Randomly.fromList(validOptions);
		}
		if (filterColumns(PostgresDataType.BOOLEAN).isEmpty() && option == BooleanExpression.COLUMN) {
			option = BooleanExpression.CONSTANT;
		}
		switch (option) {
		case CONSTANT:
			return generateConstant(r, PostgresDataType.BOOLEAN);
		case POSTFIX_OPERATOR:
			PostfixOperator random = PostfixOperator.getRandom();
			return PostgresPostfixOperation
					.create(generateExpression(depth + 1, Randomly.fromOptions(random.getInputDataTypes())), random);
		case COLUMN:
			return createColumnOfType(PostgresDataType.BOOLEAN);
		case IN_OPERATION:
			return inOperation(depth + 1);
		case NOT:
			return new PostgresPrefixOperation(generateBooleanExpression(depth + 1), PrefixOperator.NOT);
		case BINARY_LOGICAL_OPERATOR:
			return new PostgresBinaryLogicalOperation(generateBooleanExpression(depth + 1),
					generateBooleanExpression(depth + 1), BinaryLogicalOperator.getRandom());
		case BINARY_COMPARISON:
			PostgresDataType dataType = getMeaningfulType();
			return generateComparison(depth, dataType);
		case CAST:
			return new PostgresCastOperation(generateExpression(depth + 1), getCompoundDataType(PostgresDataType.BOOLEAN));
		case FUNCTION:
			return generateFunction(depth + 1, PostgresDataType.BOOLEAN);
		case LIKE:
			return new PostgresLikeOperation(generateExpression(depth + 1, PostgresDataType.TEXT),
					generateExpression(depth + 1, PostgresDataType.TEXT));
		case BETWEEN:
			PostgresDataType type = getMeaningfulType();
			return new PostgresBetweenOperation(generateExpression(depth + 1, type),
					generateExpression(depth + 1, type), generateExpression(depth + 1, type), Randomly.getBoolean());
		case SIMILAR_TO:
			assert !expectedResult;
			// TODO also generate the escape character
			return new PostgresSimilarTo(generateExpression(depth + 1, PostgresDataType.TEXT),
					generateExpression(depth + 1, PostgresDataType.TEXT), null);
		case POSIX_REGEX:
			assert !expectedResult;
			return new PostgresPOSIXRegularExpression(generateExpression(depth + 1, PostgresDataType.TEXT),
					generateExpression(depth + 1, PostgresDataType.TEXT), POSIXRegex.getRandom());
		case BINARY_RANGE_COMPARISON:
			// TODO element check
			return new PostgresBinaryRangeOperation(PostgresBinaryRangeComparisonOperator.getRandom(), generateExpression(depth + 1, PostgresDataType.RANGE), generateExpression(depth + 1, PostgresDataType.RANGE));
		default:
			throw new AssertionError();
		}
	}

	private PostgresDataType getMeaningfulType() {
		// make it more likely that the expression does not only consist of constant expressions
		if (Randomly.getBooleanWithSmallProbability() || columns == null || columns.isEmpty()) {
			return PostgresDataType.getRandomType();
		} else {
			return Randomly.fromList(columns).getColumnType();
		}
	}

	private PostgresExpression generateFunction(int depth, PostgresDataType type) {
		if (PostgresProvider.GENERATE_ONLY_KNOWN || Randomly.getBoolean()) {
			return generateFunctionWithKnownResult(depth, type);
		} else {
			return generateFunctionWithUnknownResult(depth, type);
		}
	}

	private PostgresExpression generateComparison(int depth, PostgresDataType dataType) {
		PostgresExpression leftExpr = generateExpression(depth + 1, dataType);
		PostgresExpression rightExpr = generateExpression(depth + 1, dataType);
		return getComparison(leftExpr, rightExpr);
	}

	private PostgresExpression getComparison(PostgresExpression leftExpr, PostgresExpression rightExpr) {
		PostgresBinaryComparisonOperation op = new PostgresBinaryComparisonOperation(leftExpr, rightExpr,
				PostgresBinaryComparisonOperation.PostgresBinaryComparisonOperator.getRandom());
		if (PostgresProvider.GENERATE_ONLY_KNOWN && op.getLeft().getExpressionType() == PostgresDataType.TEXT
				&& op.getRight().getExpressionType() == PostgresDataType.TEXT) {
			return new PostgresCollate(op, "C");
		}
		return op;
	}

	private PostgresExpression inOperation(int depth) {
		PostgresDataType type = PostgresDataType.getRandomType();
		PostgresExpression leftExpr = generateExpression(depth + 1, type);
		List<PostgresExpression> rightExpr = new ArrayList<>();
		for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
			rightExpr.add(generateExpression(depth + 1, type));
		}
		return new PostgresInOperation(leftExpr, rightExpr, Randomly.getBoolean());
	}

	public static PostgresExpression generateExpression(Randomly r, PostgresDataType type) {
		return new PostgresExpressionGenerator(r).generateExpression(0, type);
	}
	
	PostgresExpression generateExpression(int depth, PostgresDataType dataType) {
		if (dataType == PostgresDataType.REAL && Randomly.getBoolean()) {
			dataType = Randomly.fromOptions(PostgresDataType.INT, PostgresDataType.FLOAT);
		}
		if (dataType == PostgresDataType.FLOAT && Randomly.getBoolean()) {
			dataType = PostgresDataType.INT;
		}
		if (!filterColumns(dataType).isEmpty() && Randomly.getBoolean()) {
			return createColumnOfType(dataType);
		}
		if (Randomly.getBooleanWithSmallProbability() || depth > MAX_DEPTH) {
			// generic expression
			if (Randomly.getBoolean() || depth > MAX_DEPTH) {
				if (Randomly.getBooleanWithSmallProbability()) {
					return generateConstant(r, dataType);
				} else {
					if (filterColumns(dataType).isEmpty()) {
						return generateConstant(r, dataType);
					} else {
						return createColumnOfType(dataType);
					}
				}
			} else {
				if (Randomly.getBoolean()) {
					return new PostgresCastOperation(generateExpression(depth + 1), getCompoundDataType(dataType));
				} else {
					return generateFunctionWithUnknownResult(depth, dataType);
				}
			}
		} else {
//			// FIXME remove
//			if (Randomly.getBoolean()) {
//				return generateFunctionWithUnknownResult(depth, dataType);
//			}
			switch (dataType) {
			case BOOLEAN:
				return generateBooleanExpression(depth);
			case INT:
				return generateIntExpression(depth);
			case TEXT:
				return generateTextExpression(depth);
			case DECIMAL:
			case REAL:
			case FLOAT:
			case MONEY:
			case INET:
				return generateConstant(r, dataType);
			case BIT:
				return generateBitExpression(depth);
			case RANGE:
				return generateRangeExpression(depth);
			default:
				throw new AssertionError(dataType);
			}
		}
	}
	
	private static PostgresCompoundDataType getCompoundDataType(PostgresDataType type) {
		switch (type) {
		case BOOLEAN:
		case DECIMAL: // TODO
		case FLOAT:
		case INT:
		case MONEY:
		case RANGE:
		case REAL:
		case INET:
			return PostgresCompoundDataType.create(type);
		case TEXT: // TODO
		case BIT:
			if (Randomly.getBoolean()) {
				return PostgresCompoundDataType.create(type);
			} else {
				return PostgresCompoundDataType.create(type, (int) Randomly.getNotCachedInteger(1, 1000));
			}
		default:
			throw new AssertionError(type);
		}
		
	}
	
	private enum RangeExpression {
		CONSTANT, COLUMN, BINARY_OP;
	}

	private PostgresExpression generateRangeExpression(int depth) {
		RangeExpression option;
		if (depth >= MAX_DEPTH) {
			if (Randomly.getBooleanWithSmallProbability()) {
				option = RangeExpression.CONSTANT;
			} else {
				option = RangeExpression.COLUMN;
			}
		} else {
			List<RangeExpression> validOptions = new ArrayList<>(Arrays.asList(RangeExpression.values()));
			option = Randomly.fromList(validOptions);
		}
		if (option == RangeExpression.COLUMN && filterColumns(PostgresDataType.RANGE).isEmpty()) {
			option = RangeExpression.CONSTANT;
		}
		switch (option) {
		case BINARY_OP:
			return new PostgresBinaryRangeOperation(PostgresBinaryRangeOperator.getRandom(), generateExpression(depth + 1, PostgresDataType.RANGE), generateExpression(depth + 1, PostgresDataType.RANGE));
		case COLUMN:
			return createColumnOfType(PostgresDataType.RANGE);
		case CONSTANT:
			return generateConstant(r, PostgresDataType.RANGE);
		default:
			throw new AssertionError(option);
		}
	}

	private enum TextExpression {
		CONSTANT, COLUMN, CAST, FUNCTION, CONCAT, COLLATE
	}

	private PostgresExpression generateTextExpression(int depth) {
		TextExpression option;
		if (depth >= MAX_DEPTH) {
			if (Randomly.getBooleanWithSmallProbability()) {
				option = TextExpression.CONSTANT;
			} else {
				option = TextExpression.COLUMN;
			}
		} else {
			List<TextExpression> validOptions = new ArrayList<>(Arrays.asList(TextExpression.values()));
			if (expectedResult) {
				validOptions.remove(TextExpression.COLLATE);
			}
			option = Randomly.fromList(validOptions);
		}
		if (option == TextExpression.COLUMN &&  filterColumns(PostgresDataType.TEXT).isEmpty()) {
			option = TextExpression.CONSTANT;
		}

		switch (option) {
		case CONSTANT:
			return PostgresConstant.createTextConstant(r.getString());
		case COLUMN:
			return createColumnOfType(PostgresDataType.TEXT);
		case CAST:
			return new PostgresCastOperation(generateExpression(depth + 1), getCompoundDataType(PostgresDataType.TEXT));
		case FUNCTION:
			return generateFunction(depth + 1, PostgresDataType.TEXT);
		case CONCAT:
			return generateConcat(depth);
		case COLLATE:
			assert !expectedResult;
			return new PostgresCollate(generateTextExpression(depth + 1), globalState == null ? Randomly.fromOptions("C", "POSIX", "de_CH.utf8", "es_CR.utf8") : globalState.getRandomCollate());
		default:
			throw new AssertionError();
		}
	}

	private PostgresExpression generateConcat(int depth) {
		while (true) {
			PostgresExpression left = generateExpression(depth + 1);
			PostgresExpression right = generateExpression(depth + 1);
			if (left.getExpressionType() == PostgresDataType.TEXT
					|| right.getExpressionType() == PostgresDataType.TEXT) {
				return new PostgresConcatOperation(left, right);
			}
		}
	}
	
	
	private enum BitExpression {
		CONSTANT, COLUMN, BINARY_OPERATION
	};

	private PostgresExpression generateBitExpression(int depth) {
		BitExpression option;
		if (depth >= MAX_DEPTH) {
			if (Randomly.getBooleanWithSmallProbability()) {
				option = BitExpression.CONSTANT;
			} else {
				option = BitExpression.COLUMN;
			}
		} else {
			option = Randomly.fromOptions(BitExpression.values());
		}
		if (option == BitExpression.COLUMN && filterColumns(PostgresDataType.BIT).isEmpty()) {
			option = BitExpression.CONSTANT;
		}
		switch (option) {
		case BINARY_OPERATION:
			return new PostgresBinaryBitOperation(PostgresBinaryBitOperator.getRandom(), generateExpression(depth + 1, PostgresDataType.BIT), generateExpression(depth + 1, PostgresDataType.BIT));
		case COLUMN:
			return createColumnOfType(PostgresDataType.BIT);
		case CONSTANT:
			return generateConstant(r, PostgresDataType.BIT);
		default:
			throw new AssertionError();
		}
	}
	
	private enum IntExpression {
		CONSTANT, COLUMN, UNARY_OPERATION, FUNCTION, CAST, BINARY_ARITHMETIC_EXPRESSION
	}

	private PostgresExpression generateIntExpression(int depth) {
		IntExpression option;
		if (depth >= MAX_DEPTH) {
			if (Randomly.getBooleanWithSmallProbability()) {
				option = IntExpression.CONSTANT;
			} else {
				option = IntExpression.COLUMN;
			}
		} else {
			option = Randomly.fromOptions(IntExpression.values());
		}
		if (filterColumns(PostgresDataType.INT).isEmpty()) {
			option = IntExpression.CONSTANT;
		}
		switch (option) {
		case CAST:
			return new PostgresCastOperation(generateExpression(depth + 1), getCompoundDataType(PostgresDataType.INT));
		case CONSTANT:
			return generateConstant(r, PostgresDataType.INT);
		case COLUMN:
			return createColumnOfType(PostgresDataType.INT);
		case UNARY_OPERATION:
			PostgresExpression intExpression = generateIntExpression(depth + 1);
			return new PostgresPrefixOperation(intExpression,
					Randomly.getBoolean() ? PrefixOperator.UNARY_PLUS : PrefixOperator.UNARY_MINUS);
		case FUNCTION:
			return generateFunction(depth + 1, PostgresDataType.INT);
		case BINARY_ARITHMETIC_EXPRESSION:
			return new PostgresBinaryArithmeticOperation(generateIntExpression(depth + 1),
					generateIntExpression(depth + 1), PostgresBinaryOperator.getRandom());
		default:
			throw new AssertionError();
		}
	}

	private PostgresExpression createColumnOfType(PostgresDataType type) {
		List<PostgresColumn> columns = filterColumns(type);
		PostgresColumn fromList = Randomly.fromList(columns);
		PostgresConstant value = rw == null ? null : rw.getValues().get(fromList);
		return PostgresColumnValue.create(fromList, value);
	}

	private final List<PostgresColumn> filterColumns(PostgresDataType type) {
		if (columns == null) {
			return Collections.emptyList();
		} else {
			return columns.stream().filter(c -> c.getColumnType() == type).collect(Collectors.toList());
		}
	}

	public static PostgresExpression generateConstant(Randomly r) {
		return generateConstant(r, Randomly.fromOptions(PostgresDataType.values()));
	}

	public static PostgresExpression generateTrueCondition(List<PostgresColumn> columns, PostgresRowValue rw,
			Randomly r) {
		PostgresExpression expr = new PostgresExpressionGenerator(r).setColumns(columns).setRowValue(rw)
				.expectedResult().generateExpression(0, PostgresDataType.BOOLEAN);
		if (expr.getExpectedValue().isNull()) {
			return PostgresPostfixOperation.create(expr, PostfixOperator.IS_NULL);
		}
		return PostgresPostfixOperation.create(expr,
				expr.getExpectedValue().cast(PostgresDataType.BOOLEAN).asBoolean() ? PostfixOperator.IS_TRUE
						: PostfixOperator.IS_FALSE);
	}

	public static PostgresExpression generateConstant(Randomly r, PostgresDataType type) {
		if (Randomly.getBooleanWithSmallProbability()) {
			return PostgresConstant.createNullConstant();
		}
//		if (Randomly.getBooleanWithSmallProbability()) {
//			return PostgresConstant.createTextConstant(r.getString());
//		}
		switch (type) {
		case INT:
			if (Randomly.getBooleanWithSmallProbability()) {
				return PostgresConstant.createTextConstant(String.valueOf(r.getInteger()));
			} else {
				return PostgresConstant.createIntConstant(r.getInteger());
			}
		case BOOLEAN:
			if (Randomly.getBooleanWithSmallProbability()) {
				return PostgresConstant
						.createTextConstant(Randomly.fromOptions("TR", "TRUE", "FA", "FALSE", "0", "1", "ON", "off"));
			} else {
				return PostgresConstant.createBooleanConstant(Randomly.getBoolean());
			}
		case TEXT:
			return PostgresConstant.createTextConstant(r.getString());
		case DECIMAL:
			return PostgresConstant.createDecimalConstant(r.getRandomBigDecimal());
		case FLOAT:
			return PostgresConstant.createFloatConstant((float) r.getDouble());
		case REAL:
			return PostgresConstant.createDoubleConstant(r.getDouble());
		case RANGE:
			return PostgresConstant.createRange(r.getInteger(), Randomly.getBoolean(), r.getInteger(), Randomly.getBoolean());
		case MONEY:
			return new PostgresCastOperation(generateConstant(r, PostgresDataType.FLOAT), getCompoundDataType(PostgresDataType.MONEY));
		case INET:
			return PostgresConstant.createInetConstant(getRandomInet(r));
		case BIT:
			return PostgresConstant.createBitConstant(r.getInteger());
		default:
			throw new AssertionError(type);
		}
	}

	private static String getRandomInet(Randomly r) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < 4; i++) {
			if (i != 0) {
				sb.append('.');
			}
			sb.append(r.getInteger() & 255);
		}
		return sb.toString();
	}

	public static PostgresExpression generateExpression(Randomly r, List<PostgresColumn> columns,
			PostgresDataType type) {
		return new PostgresExpressionGenerator(r).setColumns(columns).generateExpression(0, type);
	}

	public static PostgresExpression generateExpression(Randomly r, List<PostgresColumn> columns, PostgresDataType type,
			PostgresRowValue rw) {
		return new PostgresExpressionGenerator(r).setColumns(columns).setRowValue(rw).generateExpression(0, type);
	}

	public static PostgresExpression generateExpression(Randomly r, List<PostgresColumn> columns) {
		return new PostgresExpressionGenerator(r).setColumns(columns).generateExpression(0);

	}

	public List<PostgresExpression> generateExpressions(int nr) {
		List<PostgresExpression> expressions = new ArrayList<>();
		for (int i = 0; i < nr; i++) {
			expressions.add(generateExpression(0));
		}
		return expressions;
	}

	public PostgresExpression generateExpression(PostgresDataType dataType) {
		return generateExpression(0, dataType);
	}

	public PostgresExpressionGenerator setGlobalState(PostgresGlobalState globalState) {
		this.globalState = globalState;
		return this;
	}

}
