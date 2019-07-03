package postgres.gen;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import lama.IgnoreMeException;
import lama.Randomly;
import lama.postgres.ast.PostgresComputableFunction;
import lama.postgres.ast.PostgresComputableFunction.PostgresFunction;
import postgres.PostgresSchema.PostgresColumn;
import postgres.PostgresSchema.PostgresDataType;
import postgres.PostgresSchema.PostgresRowValue;
import postgres.ast.PostgresBinaryComparisonOperation;
import postgres.ast.PostgresBinaryLogicalOperation;
import postgres.ast.PostgresBinaryLogicalOperation.BinaryLogicalOperator;
import postgres.ast.PostgresCastOperation;
import postgres.ast.PostgresColumnValue;
import postgres.ast.PostgresConstant;
import postgres.ast.PostgresExpression;
import postgres.ast.PostgresLikeOperation;
import postgres.ast.PostgresPostfixOperation;
import postgres.ast.PostgresPostfixOperation.PostfixOperator;
import postgres.ast.PostgresPrefixOperation;
import postgres.ast.PostgresPrefixOperation.PrefixOperator;

public class PostgresExpressionGenerator {

	private final int MAX_DEPTH = 10;

	private Randomly r;

	private List<PostgresColumn> columns;

	private PostgresRowValue rw;

	public PostgresExpressionGenerator(Randomly r) {
		this.r = r;
	}

	public PostgresExpressionGenerator(Randomly r, List<PostgresColumn> columns) {
		this.r = r;
		this.columns = columns;
	}

	public PostgresExpressionGenerator(Randomly r, List<PostgresColumn> columns, PostgresRowValue rw) {
		this.r = r;
		this.columns = columns;
		this.rw = rw;
	}

	public static PostgresExpression generateExpression(Randomly r) {
		return new PostgresExpressionGenerator(r).generateExpression(0);
	}

	private PostgresExpression generateExpression(int depth) {
		switch (Randomly.fromOptions(PostgresDataType.values())) {
		case BOOLEAN:
			return generateBooleanExpression(depth);
		case INT:
			return generateIntExpression(depth);
		case TEXT:
			return generateTextExpression(depth);
		default:
			throw new AssertionError();
		}
	}

	private enum BooleanExpression {
		CONSTANT, POSTFIX_OPERATOR, COLUMN, NOT, BINARY_LOGICAL_OPERATOR, BINARY_COMPARISON, FUNCTION, CAST, LIKE;
	}

	private PostgresExpression generateFunction(int depth, PostgresDataType type) {
		List<PostgresFunction> functions = Stream.of(PostgresComputableFunction.PostgresFunction.values())
				.filter(f -> f.supportsReturnType(type)).collect(Collectors.toList());
		if (functions.isEmpty()) {
			throw new IgnoreMeException();
		}
		PostgresFunction randomFunction = Randomly.fromList(functions);
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
		PostgresComputableFunction f = new PostgresComputableFunction(randomFunction, type, args);
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
			option = Randomly.fromOptions(BooleanExpression.values());
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
		case NOT:
			return new PostgresPrefixOperation(generateBooleanExpression(depth + 1), PrefixOperator.NOT);
		case BINARY_LOGICAL_OPERATOR:
			return new PostgresBinaryLogicalOperation(generateBooleanExpression(depth + 1),
					generateBooleanExpression(depth + 1), BinaryLogicalOperator.getRandom());
		case BINARY_COMPARISON:
			PostgresDataType dataType = PostgresDataType.getRandomType();
			PostgresExpression leftExpr = generateExpression(depth + 1, dataType);
			PostgresExpression rightExpr;
//			if (Randomly.getBoolean()) {
//				if (Randomly.getBoolean()) {
//					rightExpr = leftExpr.getExpectedValue();
//				} else {
//					rightExpr = leftExpr;
//				}
//			} else {
			rightExpr = generateExpression(depth + 1, dataType);
//			}
			return new PostgresBinaryComparisonOperation(leftExpr, rightExpr,
					PostgresBinaryComparisonOperation.PostgresBinaryComparisonOperator.getRandom());
		case CAST:
			return new PostgresCastOperation(generateExpression(depth + 1), PostgresDataType.BOOLEAN);
		case FUNCTION:
			return generateFunction(depth + 1, PostgresDataType.BOOLEAN);
		case LIKE:
			return new PostgresLikeOperation(generateExpression(depth + 1, PostgresDataType.TEXT),
					generateExpression(depth + 1, PostgresDataType.TEXT));
		default:
			throw new AssertionError();
		}
	}

	public static PostgresExpression generateExpression(Randomly r, PostgresDataType type) {
		return new PostgresExpressionGenerator(r).generateExpression(0, type);
	}

	private PostgresExpression generateExpression(int depth, PostgresDataType dataType) {
		switch (dataType) {
		case BOOLEAN:
			return generateBooleanExpression(depth);
		case INT:
			return generateIntExpression(depth);
		case TEXT:
			return generateTextExpression(depth);
		default:
			throw new AssertionError(dataType);
		}
	}

	private enum TextExpression {
		CONSTANT, COLUMN, CAST, FUNCTION
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
			option = Randomly.fromOptions(TextExpression.values());
		}
		if (filterColumns(PostgresDataType.TEXT).isEmpty()) {
			option = TextExpression.CONSTANT;
		}

		switch (option) {
		case CONSTANT:
			return PostgresConstant.createTextConstant(r.getString());
		case COLUMN:
			return createColumnOfType(PostgresDataType.TEXT);
		case CAST:
			return new PostgresCastOperation(generateExpression(depth + 1), PostgresDataType.TEXT);
		case FUNCTION:
			return generateFunction(depth + 1, PostgresDataType.TEXT);
		default:
			throw new AssertionError();
		}
	}

	private enum IntExpression {
		CONSTANT, COLUMN, UNARY_OPERATION, FUNCTION, CAST
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
			return new PostgresCastOperation(generateExpression(depth + 1), PostgresDataType.INT);
		case CONSTANT:
			return generateConstant(r, PostgresDataType.INT);
		case COLUMN:
			return createColumnOfType(PostgresDataType.INT);
		case UNARY_OPERATION:
			PostgresExpression intExpression = generateIntExpression(depth + 1);
			return new PostgresPrefixOperation(intExpression,
					true ? PrefixOperator.UNARY_PLUS : PrefixOperator.UNARY_MINUS);
		case FUNCTION:
			return generateFunction(depth + 1, PostgresDataType.INT);
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

	public static PostgresConstant generateConstant(Randomly r) {
		return generateConstant(r, Randomly.fromOptions(PostgresDataType.values()));
	}

	public static PostgresExpression generateTrueCondition(List<PostgresColumn> columns, PostgresRowValue rw,
			Randomly r) {
		PostgresExpression expr = generateExpression(r, columns, PostgresDataType.BOOLEAN, rw);
		if (expr.getExpectedValue().isNull()) {
			return PostgresPostfixOperation.create(expr, PostfixOperator.IS_NULL);
		}
		return PostgresPostfixOperation.create(expr,
				expr.getExpectedValue().cast(PostgresDataType.BOOLEAN).asBoolean() ? PostfixOperator.IS_TRUE
						: PostfixOperator.IS_FALSE);
	}

	public static PostgresConstant generateConstant(Randomly r, PostgresDataType type) {
		if (Randomly.getBooleanWithSmallProbability()) {
			return PostgresConstant.createNullConstant();
		}
		if (Randomly.getBooleanWithSmallProbability()) {
			return PostgresConstant.createTextConstant(r.getString());
		}
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
		default:
			throw new AssertionError();
		}
	}

	public static PostgresExpression generateExpression(Randomly r, List<PostgresColumn> columns,
			PostgresDataType type) {
		return new PostgresExpressionGenerator(r, columns).generateExpression(0, type);
	}

	public static PostgresExpression generateExpression(Randomly r, List<PostgresColumn> columns, PostgresDataType type,
			PostgresRowValue rw) {
		return new PostgresExpressionGenerator(r, columns, rw).generateExpression(0, type);
	}

	public static PostgresExpression generateExpression(Randomly r, List<PostgresColumn> columns) {
		return new PostgresExpressionGenerator(r, columns).generateExpression(0);

	}

}
