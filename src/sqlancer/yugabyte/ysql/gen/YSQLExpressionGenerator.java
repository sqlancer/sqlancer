package sqlancer.yugabyte.ysql.gen;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.gen.ExpressionGenerator;
import sqlancer.yugabyte.ysql.YSQLCompoundDataType;
import sqlancer.yugabyte.ysql.YSQLGlobalState;
import sqlancer.yugabyte.ysql.YSQLProvider;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLColumn;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLDataType;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLRowValue;
import sqlancer.yugabyte.ysql.ast.YSQLAggregate;
import sqlancer.yugabyte.ysql.ast.YSQLBetweenOperation;
import sqlancer.yugabyte.ysql.ast.YSQLBinaryArithmeticOperation;
import sqlancer.yugabyte.ysql.ast.YSQLBinaryBitOperation;
import sqlancer.yugabyte.ysql.ast.YSQLBinaryComparisonOperation;
import sqlancer.yugabyte.ysql.ast.YSQLBinaryLogicalOperation;
import sqlancer.yugabyte.ysql.ast.YSQLBinaryRangeOperation;
import sqlancer.yugabyte.ysql.ast.YSQLCastOperation;
import sqlancer.yugabyte.ysql.ast.YSQLColumnValue;
import sqlancer.yugabyte.ysql.ast.YSQLConcatOperation;
import sqlancer.yugabyte.ysql.ast.YSQLConstant;
import sqlancer.yugabyte.ysql.ast.YSQLExpression;
import sqlancer.yugabyte.ysql.ast.YSQLFunction;
import sqlancer.yugabyte.ysql.ast.YSQLFunctionWithUnknownResult;
import sqlancer.yugabyte.ysql.ast.YSQLInOperation;
import sqlancer.yugabyte.ysql.ast.YSQLOrderByTerm;
import sqlancer.yugabyte.ysql.ast.YSQLPOSIXRegularExpression;
import sqlancer.yugabyte.ysql.ast.YSQLPostfixOperation;
import sqlancer.yugabyte.ysql.ast.YSQLPrefixOperation;
import sqlancer.yugabyte.ysql.ast.YSQLSimilarTo;

public class YSQLExpressionGenerator implements ExpressionGenerator<YSQLExpression> {

    private final int maxDepth;

    private final Randomly r;
    private final Map<String, Character> functionsAndTypes;
    private final List<Character> allowedFunctionTypes;
    private List<YSQLColumn> columns;
    private YSQLRowValue rw;
    private boolean expectedResult;
    private YSQLGlobalState globalState;
    private boolean allowAggregateFunctions;

    public YSQLExpressionGenerator(YSQLGlobalState globalState) {
        this.r = globalState.getRandomly();
        this.maxDepth = globalState.getOptions().getMaxExpressionDepth();
        this.globalState = globalState;
        this.functionsAndTypes = globalState.getFunctionsAndTypes();
        this.allowedFunctionTypes = globalState.getAllowedFunctionTypes();
    }

    public static YSQLExpression generateExpression(YSQLGlobalState globalState, YSQLDataType type) {
        return new YSQLExpressionGenerator(globalState).generateExpression(0, type);
    }

    private static YSQLCompoundDataType getCompoundDataType(YSQLDataType type) {
        switch (type) {
        case BOOLEAN:
        case DECIMAL: // TODO
        case FLOAT:
        case INT:
        case MONEY:
        case RANGE:
        case REAL:
        case INET:
        case BYTEA:
            return YSQLCompoundDataType.create(type);
        case TEXT: // TODO
        case BIT:
            if (Randomly.getBoolean()
                    || YSQLProvider.generateOnlyKnown /*
                                                       * The PQS implementation does not check for size specifications
                                                       */) {
                return YSQLCompoundDataType.create(type);
            } else {
                return YSQLCompoundDataType.create(type, (int) Randomly.getNotCachedInteger(1, 1000));
            }
        default:
            throw new AssertionError(type);
        }

    }

    public static YSQLExpression generateConstant(Randomly r, YSQLDataType type) {
        if (Randomly.getBooleanWithSmallProbability()) {
            return YSQLConstant.createNullConstant();
        }
        // if (Randomly.getBooleanWithSmallProbability()) {
        // return YSQLConstant.createTextConstant(r.getString());
        // }
        switch (type) {
        case INT:
            if (Randomly.getBooleanWithSmallProbability()) {
                return YSQLConstant.createTextConstant(String.valueOf(r.getInteger()));
            } else {
                return YSQLConstant.createIntConstant(r.getInteger());
            }
        case BOOLEAN:
            if (Randomly.getBooleanWithSmallProbability() && !YSQLProvider.generateOnlyKnown) {
                return YSQLConstant
                        .createTextConstant(Randomly.fromOptions("TR", "TRUE", "FA", "FALSE", "0", "1", "ON", "off"));
            } else {
                return YSQLConstant.createBooleanConstant(Randomly.getBoolean());
            }
        case TEXT:
            return YSQLConstant.createTextConstant(r.getString());
        case DECIMAL:
            return YSQLConstant.createDecimalConstant(r.getRandomBigDecimal());
        case FLOAT:
            return YSQLConstant.createFloatConstant((float) r.getDouble());
        case REAL:
            return YSQLConstant.createDoubleConstant(r.getDouble());
        case RANGE:
            return YSQLConstant.createRange(r.getInteger(), Randomly.getBoolean(), r.getInteger(),
                    Randomly.getBoolean());
        case MONEY:
            return new YSQLCastOperation(generateConstant(r, YSQLDataType.FLOAT),
                    getCompoundDataType(YSQLDataType.MONEY));
        case INET:
            return YSQLConstant.createInetConstant(getRandomInet(r));
        case BIT:
            return YSQLConstant.createBitConstant(r.getInteger());
        case BYTEA:
            return YSQLConstant.createByteConstant(String.valueOf(r.getInteger()));
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

    public static YSQLExpression generateExpression(YSQLGlobalState globalState, List<YSQLColumn> columns,
            YSQLDataType type) {
        return new YSQLExpressionGenerator(globalState).setColumns(columns).generateExpression(0, type);
    }

    public static YSQLExpression generateExpression(YSQLGlobalState globalState, List<YSQLColumn> columns) {
        return new YSQLExpressionGenerator(globalState).setColumns(columns).generateExpression(0);

    }

    public YSQLExpressionGenerator setColumns(List<YSQLColumn> columns) {
        this.columns = columns;
        return this;
    }

    public YSQLExpressionGenerator setRowValue(YSQLRowValue rw) {
        this.rw = rw;
        return this;
    }

    public YSQLExpression generateExpression(int depth) {
        return generateExpression(depth, YSQLDataType.getRandomType());
    }

    public List<YSQLExpression> generateOrderBy() {
        List<YSQLExpression> orderBys = new ArrayList<>();
        for (int i = 0; i < Randomly.smallNumber(); i++) {
            orderBys.add(new YSQLOrderByTerm(YSQLColumnValue.create(Randomly.fromList(columns), null),
                    YSQLOrderByTerm.YSQLOrder.getRandomOrder()));
        }
        return orderBys;
    }

    private YSQLExpression generateFunctionWithUnknownResult(int depth, YSQLDataType type) {
        List<YSQLFunctionWithUnknownResult> supportedFunctions = YSQLFunctionWithUnknownResult
                .getSupportedFunctions(type);
        // filters functions by allowed type (STABLE 's', IMMUTABLE 'i', VOLATILE 'v')
        supportedFunctions = supportedFunctions.stream()
                .filter(f -> allowedFunctionTypes.contains(functionsAndTypes.get(f.getName())))
                .collect(Collectors.toList());
        if (supportedFunctions.isEmpty()) {
            throw new IgnoreMeException();
        }
        YSQLFunctionWithUnknownResult randomFunction = Randomly.fromList(supportedFunctions);
        return new YSQLFunction(randomFunction, type, randomFunction.getArguments(type, this, depth + 1));
    }

    private YSQLExpression generateFunctionWithKnownResult(int depth, YSQLDataType type) {
        List<YSQLFunction.YSQLFunctionWithResult> functions = Stream.of(YSQLFunction.YSQLFunctionWithResult.values())
                .filter(f -> f.supportsReturnType(type)).collect(Collectors.toList());
        // filters functions by allowed type (STABLE 's', IMMUTABLE 'i', VOLATILE 'v')
        functions = functions.stream().filter(f -> allowedFunctionTypes.contains(functionsAndTypes.get(f.getName())))
                .collect(Collectors.toList());
        if (functions.isEmpty()) {
            throw new IgnoreMeException();
        }
        YSQLFunction.YSQLFunctionWithResult randomFunction = Randomly.fromList(functions);
        int nrArgs = randomFunction.getNrArgs();
        if (randomFunction.isVariadic()) {
            nrArgs += Randomly.smallNumber();
        }
        YSQLDataType[] argTypes = randomFunction.getInputTypesForReturnType(type, nrArgs);
        YSQLExpression[] args = new YSQLExpression[nrArgs];
        do {
            for (int i = 0; i < args.length; i++) {
                args[i] = generateExpression(depth + 1, argTypes[i]);
            }
        } while (!randomFunction.checkArguments(args));
        return new YSQLFunction(randomFunction, type, args);
    }

    private YSQLExpression generateBooleanExpression(int depth) {
        List<BooleanExpression> validOptions = new ArrayList<>(Arrays.asList(BooleanExpression.values()));
        if (YSQLProvider.generateOnlyKnown) {
            validOptions.remove(BooleanExpression.SIMILAR_TO);
            validOptions.remove(BooleanExpression.POSIX_REGEX);
            validOptions.remove(BooleanExpression.BINARY_RANGE_COMPARISON);
        }
        BooleanExpression option = Randomly.fromList(validOptions);
        switch (option) {
        case POSTFIX_OPERATOR:
            YSQLPostfixOperation.PostfixOperator random = YSQLPostfixOperation.PostfixOperator.getRandom();
            return YSQLPostfixOperation
                    .create(generateExpression(depth + 1, Randomly.fromOptions(random.getInputDataTypes())), random);
        case IN_OPERATION:
            return inOperation(depth + 1);
        case NOT:
            return new YSQLPrefixOperation(generateExpression(depth + 1, YSQLDataType.BOOLEAN),
                    YSQLPrefixOperation.PrefixOperator.NOT);
        case BINARY_LOGICAL_OPERATOR:
            YSQLExpression first = generateExpression(depth + 1, YSQLDataType.BOOLEAN);
            int nr = Randomly.smallNumber() + 1;
            for (int i = 0; i < nr; i++) {
                first = new YSQLBinaryLogicalOperation(first, generateExpression(depth + 1, YSQLDataType.BOOLEAN),
                        YSQLBinaryLogicalOperation.BinaryLogicalOperator.getRandom());
            }
            return first;
        case BINARY_COMPARISON:
            YSQLDataType dataType = getMeaningfulType();
            return generateComparison(depth, dataType);
        case CAST:
            return new YSQLCastOperation(generateExpression(depth + 1), getCompoundDataType(YSQLDataType.BOOLEAN));
        case FUNCTION:
            return generateFunction(depth + 1, YSQLDataType.BOOLEAN);
        case BETWEEN:
            YSQLDataType type = getMeaningfulType();
            return new YSQLBetweenOperation(generateExpression(depth + 1, type), generateExpression(depth + 1, type),
                    generateExpression(depth + 1, type), Randomly.getBoolean());
        case SIMILAR_TO:
            assert !expectedResult;
            // TODO also generate the escape character
            return new YSQLSimilarTo(generateExpression(depth + 1, YSQLDataType.TEXT),
                    generateExpression(depth + 1, YSQLDataType.TEXT), null);
        case POSIX_REGEX:
            assert !expectedResult;
            return new YSQLPOSIXRegularExpression(generateExpression(depth + 1, YSQLDataType.TEXT),
                    generateExpression(depth + 1, YSQLDataType.TEXT),
                    YSQLPOSIXRegularExpression.POSIXRegex.getRandom());
        case BINARY_RANGE_COMPARISON:
            // TODO element check
            return new YSQLBinaryRangeOperation(YSQLBinaryRangeOperation.YSQLBinaryRangeComparisonOperator.getRandom(),
                    generateExpression(depth + 1, YSQLDataType.RANGE),
                    generateExpression(depth + 1, YSQLDataType.RANGE));
        default:
            throw new AssertionError();
        }
    }

    private YSQLDataType getMeaningfulType() {
        // make it more likely that the expression does not only consist of constant
        // expressions
        if (Randomly.getBooleanWithSmallProbability() || columns == null || columns.isEmpty()) {
            return YSQLDataType.getRandomType();
        } else {
            return Randomly.fromList(columns).getType();
        }
    }

    private YSQLExpression generateFunction(int depth, YSQLDataType type) {
        if (YSQLProvider.generateOnlyKnown || Randomly.getBoolean()) {
            return generateFunctionWithKnownResult(depth, type);
        } else {
            return generateFunctionWithUnknownResult(depth, type);
        }
    }

    private YSQLExpression generateComparison(int depth, YSQLDataType dataType) {
        YSQLExpression leftExpr = generateExpression(depth + 1, dataType);
        YSQLExpression rightExpr = generateExpression(depth + 1, dataType);
        return getComparison(leftExpr, rightExpr);
    }

    private YSQLExpression getComparison(YSQLExpression leftExpr, YSQLExpression rightExpr) {
        return new YSQLBinaryComparisonOperation(leftExpr, rightExpr,
                YSQLBinaryComparisonOperation.YSQLBinaryComparisonOperator.getRandom());
    }

    private YSQLExpression inOperation(int depth) {
        YSQLDataType type = YSQLDataType.getRandomType();
        YSQLExpression leftExpr = generateExpression(depth + 1, type);
        List<YSQLExpression> rightExpr = new ArrayList<>();
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
            rightExpr.add(generateExpression(depth + 1, type));
        }
        return new YSQLInOperation(leftExpr, rightExpr, Randomly.getBoolean());
    }

    public YSQLExpression generateExpression(int depth, YSQLDataType originalType) {
        YSQLDataType dataType = originalType;
        if (dataType == YSQLDataType.REAL && Randomly.getBoolean()) {
            dataType = Randomly.fromOptions(YSQLDataType.INT, YSQLDataType.FLOAT);
        }
        if (dataType == YSQLDataType.FLOAT && Randomly.getBoolean()) {
            dataType = YSQLDataType.INT;
        }
        return generateExpressionInternal(depth, dataType);
    }

    private YSQLExpression generateExpressionInternal(int depth, YSQLDataType dataType) throws AssertionError {
        if (allowAggregateFunctions && Randomly.getBoolean()) {
            allowAggregateFunctions = false; // aggregate function calls cannot be nested
            return getAggregate(dataType);
        }
        if (Randomly.getBooleanWithRatherLowProbability() || depth > maxDepth) {
            // generic expression
            if (Randomly.getBoolean() || depth > maxDepth) {
                if (Randomly.getBooleanWithRatherLowProbability()) {
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
                    return new YSQLCastOperation(generateExpression(depth + 1), getCompoundDataType(dataType));
                } else {
                    return generateFunctionWithUnknownResult(depth, dataType);
                }
            }
        } else {
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
            case BYTEA:
                return generateByteExpression();
            case BIT:
                return generateBitExpression(depth);
            case RANGE:
                return generateRangeExpression(depth);
            default:
                throw new AssertionError(dataType);
            }
        }
    }

    private YSQLExpression generateRangeExpression(int depth) {
        RangeExpression option;
        List<RangeExpression> validOptions = new ArrayList<>(Arrays.asList(RangeExpression.values()));
        option = Randomly.fromList(validOptions);
        switch (option) {
        case BINARY_OP:
            return new YSQLBinaryRangeOperation(YSQLBinaryRangeOperation.YSQLBinaryRangeOperator.getRandom(),
                    generateExpression(depth + 1, YSQLDataType.RANGE),
                    generateExpression(depth + 1, YSQLDataType.RANGE));
        default:
            throw new AssertionError(option);
        }
    }

    private YSQLExpression generateTextExpression(int depth) {
        TextExpression option;
        List<TextExpression> validOptions = new ArrayList<>(Arrays.asList(TextExpression.values()));
        option = Randomly.fromList(validOptions);

        switch (option) {
        case CAST:
            return new YSQLCastOperation(generateExpression(depth + 1), getCompoundDataType(YSQLDataType.TEXT));
        case FUNCTION:
            return generateFunction(depth + 1, YSQLDataType.TEXT);
        case CONCAT:
            return generateConcat(depth);
        default:
            throw new AssertionError();
        }
    }

    private YSQLExpression generateConcat(int depth) {
        YSQLExpression left = generateExpression(depth + 1, YSQLDataType.TEXT);
        YSQLExpression right = generateExpression(depth + 1);
        return new YSQLConcatOperation(left, right);
    }

    private YSQLExpression generateByteExpression() {
        return YSQLConstant.createByteConstant("Th\\000omas");
    }

    private YSQLExpression generateBitExpression(int depth) {
        BitExpression option;
        option = Randomly.fromOptions(BitExpression.values());
        switch (option) {
        case BINARY_OPERATION:
            return new YSQLBinaryBitOperation(YSQLBinaryBitOperation.YSQLBinaryBitOperator.getRandom(),
                    generateExpression(depth + 1, YSQLDataType.BIT), generateExpression(depth + 1, YSQLDataType.BIT));
        default:
            throw new AssertionError();
        }
    }

    private YSQLExpression generateIntExpression(int depth) {
        IntExpression option;
        option = Randomly.fromOptions(IntExpression.values());
        switch (option) {
        case CAST:
            return new YSQLCastOperation(generateExpression(depth + 1), getCompoundDataType(YSQLDataType.INT));
        case UNARY_OPERATION:
            YSQLExpression intExpression = generateExpression(depth + 1, YSQLDataType.INT);
            return new YSQLPrefixOperation(intExpression, Randomly.getBoolean()
                    ? YSQLPrefixOperation.PrefixOperator.UNARY_PLUS : YSQLPrefixOperation.PrefixOperator.UNARY_MINUS);
        case FUNCTION:
            return generateFunction(depth + 1, YSQLDataType.INT);
        case BINARY_ARITHMETIC_EXPRESSION:
            return new YSQLBinaryArithmeticOperation(generateExpression(depth + 1, YSQLDataType.INT),
                    generateExpression(depth + 1, YSQLDataType.INT),
                    YSQLBinaryArithmeticOperation.YSQLBinaryOperator.getRandom());
        default:
            throw new AssertionError();
        }
    }

    private YSQLExpression createColumnOfType(YSQLDataType type) {
        List<YSQLColumn> columns = filterColumns(type);
        YSQLColumn fromList = Randomly.fromList(columns);
        YSQLConstant value = rw == null ? null : rw.getValues().get(fromList);
        return YSQLColumnValue.create(fromList, value);
    }

    final List<YSQLColumn> filterColumns(YSQLDataType type) {
        if (columns == null) {
            return Collections.emptyList();
        } else {
            return columns.stream().filter(c -> c.getType() == type).collect(Collectors.toList());
        }
    }

    public YSQLExpression generateExpressionWithExpectedResult(YSQLDataType type) {
        this.expectedResult = true;
        YSQLExpressionGenerator gen = new YSQLExpressionGenerator(globalState).setColumns(columns).setRowValue(rw);
        YSQLExpression expr;
        do {
            expr = gen.generateExpression(type);
        } while (expr.getExpectedValue() == null);
        return expr;
    }

    public List<YSQLExpression> generateExpressions(int nr) {
        List<YSQLExpression> expressions = new ArrayList<>();
        for (int i = 0; i < nr; i++) {
            expressions.add(generateExpression(0));
        }
        return expressions;
    }

    public YSQLExpression generateExpression(YSQLDataType dataType) {
        return generateExpression(0, dataType);
    }

    public YSQLExpressionGenerator setGlobalState(YSQLGlobalState globalState) {
        this.globalState = globalState;
        return this;
    }

    public YSQLExpression generateHavingClause() {
        this.allowAggregateFunctions = true;
        YSQLExpression expression = generateExpression(YSQLDataType.BOOLEAN);
        this.allowAggregateFunctions = false;
        return expression;
    }

    public YSQLExpression generateAggregate() {
        return getAggregate(YSQLDataType.getRandomType());
    }

    private YSQLExpression getAggregate(YSQLDataType dataType) {
        List<YSQLAggregate.YSQLAggregateFunction> aggregates = YSQLAggregate.YSQLAggregateFunction
                .getAggregates(dataType);
        YSQLAggregate.YSQLAggregateFunction agg = Randomly.fromList(aggregates);
        return generateArgsForAggregate(dataType, agg);
    }

    public YSQLAggregate generateArgsForAggregate(YSQLDataType dataType, YSQLAggregate.YSQLAggregateFunction agg) {
        List<YSQLDataType> types = agg.getTypes(dataType);
        List<YSQLExpression> args = new ArrayList<>();
        for (YSQLDataType argType : types) {
            args.add(generateExpression(argType));
        }
        return new YSQLAggregate(args, agg);
    }

    public YSQLExpressionGenerator allowAggregates(boolean value) {
        allowAggregateFunctions = value;
        return this;
    }

    @Override
    public YSQLExpression generatePredicate() {
        return generateExpression(YSQLDataType.BOOLEAN);
    }

    @Override
    public YSQLExpression negatePredicate(YSQLExpression predicate) {
        return new YSQLPrefixOperation(predicate, YSQLPrefixOperation.PrefixOperator.NOT);
    }

    @Override
    public YSQLExpression isNull(YSQLExpression expr) {
        return new YSQLPostfixOperation(expr, YSQLPostfixOperation.PostfixOperator.IS_NULL);
    }

    private enum BooleanExpression {
        POSTFIX_OPERATOR, NOT, BINARY_LOGICAL_OPERATOR, BINARY_COMPARISON, FUNCTION, CAST, BETWEEN, IN_OPERATION,
        SIMILAR_TO, POSIX_REGEX, BINARY_RANGE_COMPARISON
    }

    private enum RangeExpression {
        BINARY_OP
    }

    private enum TextExpression {
        CAST, FUNCTION, CONCAT
    }

    private enum BitExpression {
        BINARY_OPERATION
    }

    private enum IntExpression {
        UNARY_OPERATION, FUNCTION, CAST, BINARY_ARITHMETIC_EXPRESSION
    }

}
