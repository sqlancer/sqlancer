package sqlancer.materialize.gen;

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
import sqlancer.materialize.MaterializeCompoundDataType;
import sqlancer.materialize.MaterializeGlobalState;
import sqlancer.materialize.MaterializeProvider;
import sqlancer.materialize.MaterializeSchema.MaterializeColumn;
import sqlancer.materialize.MaterializeSchema.MaterializeDataType;
import sqlancer.materialize.MaterializeSchema.MaterializeRowValue;
import sqlancer.materialize.ast.MaterializeAggregate;
import sqlancer.materialize.ast.MaterializeAggregate.MaterializeAggregateFunction;
import sqlancer.materialize.ast.MaterializeBetweenOperation;
import sqlancer.materialize.ast.MaterializeBinaryArithmeticOperation;
import sqlancer.materialize.ast.MaterializeBinaryArithmeticOperation.MaterializeBinaryOperator;
import sqlancer.materialize.ast.MaterializeBinaryBitOperation;
import sqlancer.materialize.ast.MaterializeBinaryBitOperation.MaterializeBinaryBitOperator;
import sqlancer.materialize.ast.MaterializeBinaryComparisonOperation;
import sqlancer.materialize.ast.MaterializeBinaryLogicalOperation;
import sqlancer.materialize.ast.MaterializeBinaryLogicalOperation.BinaryLogicalOperator;
import sqlancer.materialize.ast.MaterializeCastOperation;
import sqlancer.materialize.ast.MaterializeColumnValue;
import sqlancer.materialize.ast.MaterializeConcatOperation;
import sqlancer.materialize.ast.MaterializeConstant;
import sqlancer.materialize.ast.MaterializeExpression;
import sqlancer.materialize.ast.MaterializeFunction;
import sqlancer.materialize.ast.MaterializeFunction.MaterializeFunctionWithResult;
import sqlancer.materialize.ast.MaterializeFunctionWithUnknownResult;
import sqlancer.materialize.ast.MaterializeInOperation;
import sqlancer.materialize.ast.MaterializeLikeOperation;
import sqlancer.materialize.ast.MaterializeOrderByTerm;
import sqlancer.materialize.ast.MaterializeOrderByTerm.MaterializeOrder;
import sqlancer.materialize.ast.MaterializePOSIXRegularExpression;
import sqlancer.materialize.ast.MaterializePOSIXRegularExpression.POSIXRegex;
import sqlancer.materialize.ast.MaterializePostfixOperation;
import sqlancer.materialize.ast.MaterializePostfixOperation.PostfixOperator;
import sqlancer.materialize.ast.MaterializePrefixOperation;
import sqlancer.materialize.ast.MaterializePrefixOperation.PrefixOperator;

public class MaterializeExpressionGenerator implements ExpressionGenerator<MaterializeExpression> {

    private final int maxDepth;

    private final Randomly r;

    private List<MaterializeColumn> columns;

    private MaterializeRowValue rw;

    private boolean expectedResult;

    private MaterializeGlobalState globalState;

    private boolean allowAggregateFunctions;

    private final Map<String, Character> functionsAndTypes;

    private final List<Character> allowedFunctionTypes;

    public MaterializeExpressionGenerator(MaterializeGlobalState globalState) {
        this.r = globalState.getRandomly();
        this.maxDepth = globalState.getOptions().getMaxExpressionDepth();
        this.globalState = globalState;
        this.functionsAndTypes = globalState.getFunctionsAndTypes();
        this.allowedFunctionTypes = globalState.getAllowedFunctionTypes();
    }

    public MaterializeExpressionGenerator setColumns(List<MaterializeColumn> columns) {
        this.columns = columns;
        return this;
    }

    public MaterializeExpressionGenerator setRowValue(MaterializeRowValue rw) {
        this.rw = rw;
        return this;
    }

    public MaterializeExpression generateExpression(int depth) {
        return generateExpression(depth, MaterializeDataType.getRandomType());
    }

    public List<MaterializeExpression> generateOrderBy() {
        List<MaterializeExpression> orderBys = new ArrayList<>();
        for (int i = 0; i < Randomly.smallNumber(); i++) {
            orderBys.add(new MaterializeOrderByTerm(MaterializeColumnValue.create(Randomly.fromList(columns), null),
                    MaterializeOrder.getRandomOrder()));
        }
        return orderBys;
    }

    private enum BooleanExpression {
        POSTFIX_OPERATOR, NOT, BINARY_LOGICAL_OPERATOR, BINARY_COMPARISON, FUNCTION, LIKE, BETWEEN, IN_OPERATION,
        POSIX_REGEX;
    }

    private MaterializeExpression generateFunctionWithUnknownResult(int depth, MaterializeDataType type) {
        List<MaterializeFunctionWithUnknownResult> supportedFunctions = MaterializeFunctionWithUnknownResult
                .getSupportedFunctions(type);
        // filters functions by allowed type (STABLE 's', IMMUTABLE 'i', VOLATILE 'v')
        supportedFunctions = supportedFunctions.stream()
                .filter(f -> allowedFunctionTypes.contains(functionsAndTypes.get(f.getName())))
                .collect(Collectors.toList());
        if (supportedFunctions.isEmpty()) {
            throw new IgnoreMeException();
        }
        MaterializeFunctionWithUnknownResult randomFunction = Randomly.fromList(supportedFunctions);
        return new MaterializeFunction(randomFunction, type, randomFunction.getArguments(type, this, depth + 1));
    }

    private MaterializeExpression generateFunctionWithKnownResult(int depth, MaterializeDataType type) {
        List<MaterializeFunctionWithResult> functions = Stream
                .of(MaterializeFunction.MaterializeFunctionWithResult.values()).filter(f -> f.supportsReturnType(type))
                .collect(Collectors.toList());
        // filters functions by allowed type (STABLE 's', IMMUTABLE 'i', VOLATILE 'v')
        functions = functions.stream().filter(f -> allowedFunctionTypes.contains(functionsAndTypes.get(f.getName())))
                .collect(Collectors.toList());
        if (functions.isEmpty()) {
            throw new IgnoreMeException();
        }
        MaterializeFunctionWithResult randomFunction = Randomly.fromList(functions);
        int nrArgs = randomFunction.getNrArgs();
        if (randomFunction.isVariadic()) {
            nrArgs += Randomly.smallNumber();
        }
        MaterializeDataType[] argTypes = randomFunction.getInputTypesForReturnType(type, nrArgs);
        MaterializeExpression[] args = new MaterializeExpression[nrArgs];
        do {
            for (int i = 0; i < args.length; i++) {
                args[i] = generateExpression(depth + 1, argTypes[i]);
            }
        } while (!randomFunction.checkArguments(args));
        return new MaterializeFunction(randomFunction, type, args);
    }

    private MaterializeExpression generateBooleanExpression(int depth) {
        List<BooleanExpression> validOptions = new ArrayList<>(Arrays.asList(BooleanExpression.values()));
        if (MaterializeProvider.generateOnlyKnown) {
            validOptions.remove(BooleanExpression.POSIX_REGEX);
        }
        BooleanExpression option = Randomly.fromList(validOptions);
        switch (option) {
        case POSTFIX_OPERATOR:
            PostfixOperator random = PostfixOperator.getRandom();
            return MaterializePostfixOperation
                    .create(generateExpression(depth + 1, Randomly.fromOptions(random.getInputDataTypes())), random);
        case IN_OPERATION:
            return inOperation(depth + 1);
        case NOT:
            return new MaterializePrefixOperation(generateExpression(depth + 1, MaterializeDataType.BOOLEAN),
                    PrefixOperator.NOT);
        case BINARY_LOGICAL_OPERATOR:
            MaterializeExpression first = generateExpression(depth + 1, MaterializeDataType.BOOLEAN);
            int nr = Randomly.smallNumber() + 1;
            for (int i = 0; i < nr; i++) {
                first = new MaterializeBinaryLogicalOperation(first,
                        generateExpression(depth + 1, MaterializeDataType.BOOLEAN), BinaryLogicalOperator.getRandom());
            }
            return first;
        case BINARY_COMPARISON:
            MaterializeDataType dataType = getMeaningfulType();
            return generateComparison(depth, dataType);
        case FUNCTION:
            return generateFunction(depth + 1, MaterializeDataType.BOOLEAN);
        case LIKE:
            return new MaterializeLikeOperation(generateExpression(depth + 1, MaterializeDataType.TEXT),
                    generateExpression(depth + 1, MaterializeDataType.TEXT));
        case BETWEEN:
            MaterializeDataType type = getMeaningfulType();
            return new MaterializeBetweenOperation(generateExpression(depth + 1, type),
                    generateExpression(depth + 1, type), generateExpression(depth + 1, type), Randomly.getBoolean());
        case POSIX_REGEX:
            assert !expectedResult;
            return new MaterializePOSIXRegularExpression(generateExpression(depth + 1, MaterializeDataType.TEXT),
                    generateExpression(depth + 1, MaterializeDataType.TEXT), POSIXRegex.getRandom());
        default:
            throw new AssertionError();
        }
    }

    private MaterializeDataType getMeaningfulType() {
        // make it more likely that the expression does not only consist of constant
        // expressions
        if (Randomly.getBooleanWithSmallProbability() || columns == null || columns.isEmpty()) {
            return MaterializeDataType.getRandomType();
        } else {
            return Randomly.fromList(columns).getType();
        }
    }

    private MaterializeExpression generateFunction(int depth, MaterializeDataType type) {
        if (MaterializeProvider.generateOnlyKnown || Randomly.getBoolean()) {
            return generateFunctionWithKnownResult(depth, type);
        } else {
            return generateFunctionWithUnknownResult(depth, type);
        }
    }

    private MaterializeExpression generateComparison(int depth, MaterializeDataType dataType) {
        MaterializeExpression leftExpr = generateExpression(depth + 1, dataType);
        MaterializeExpression rightExpr = generateExpression(depth + 1, dataType);
        return getComparison(leftExpr, rightExpr);
    }

    private MaterializeExpression getComparison(MaterializeExpression leftExpr, MaterializeExpression rightExpr) {
        return new MaterializeBinaryComparisonOperation(leftExpr, rightExpr,
                MaterializeBinaryComparisonOperation.MaterializeBinaryComparisonOperator.getRandom());
    }

    private MaterializeExpression inOperation(int depth) {
        MaterializeDataType type = MaterializeDataType.getRandomType();
        MaterializeExpression leftExpr = generateExpression(depth + 1, type);
        List<MaterializeExpression> rightExpr = new ArrayList<>();
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
            rightExpr.add(generateExpression(depth + 1, type));
        }
        return new MaterializeInOperation(leftExpr, rightExpr, Randomly.getBoolean());
    }

    public static MaterializeExpression generateExpression(MaterializeGlobalState globalState,
            MaterializeDataType type) {
        return new MaterializeExpressionGenerator(globalState).generateExpression(0, type);
    }

    public MaterializeExpression generateExpression(int depth, MaterializeDataType originalType) {
        MaterializeDataType dataType = originalType;
        if (dataType == MaterializeDataType.REAL && Randomly.getBoolean()) {
            dataType = Randomly.fromOptions(MaterializeDataType.INT, MaterializeDataType.FLOAT);
        }
        if (dataType == MaterializeDataType.FLOAT && Randomly.getBoolean()) {
            dataType = MaterializeDataType.INT;
        }
        if (!filterColumns(dataType).isEmpty() && Randomly.getBoolean()) {
            return createColumnOfType(dataType);
        }
        return generateExpressionInternal(depth, dataType);
    }

    private MaterializeExpression generateExpressionInternal(int depth, MaterializeDataType dataType)
            throws AssertionError {
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
                    return new MaterializeCastOperation(generateExpression(depth + 1), getCompoundDataType(dataType));
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
                return generateConstant(r, dataType);
            case BIT:
                return generateBitExpression(depth);
            default:
                throw new AssertionError(dataType);
            }
        }
    }

    private static MaterializeCompoundDataType getCompoundDataType(MaterializeDataType type) {
        switch (type) {
        case BOOLEAN:
        case DECIMAL: // TODO
        case FLOAT:
        case INT:
        case REAL:
        case BIT:
            return MaterializeCompoundDataType.create(type);
        case TEXT: // TODO
            if (Randomly.getBoolean() || MaterializeProvider.generateOnlyKnown /*
                                                                                * The PQS implementation does not check
                                                                                * for size specifications
                                                                                */) {
                return MaterializeCompoundDataType.create(type);
            } else {
                return MaterializeCompoundDataType.create(type, (int) Randomly.getNotCachedInteger(1, 1000));
            }
        default:
            throw new AssertionError(type);
        }

    }

    private enum TextExpression {
        CAST, FUNCTION, CONCAT
    }

    private MaterializeExpression generateTextExpression(int depth) {
        TextExpression option;
        List<TextExpression> validOptions = new ArrayList<>(Arrays.asList(TextExpression.values()));
        option = Randomly.fromList(validOptions);

        switch (option) {
        case CAST:
            return new MaterializeCastOperation(generateExpression(depth + 1),
                    getCompoundDataType(MaterializeDataType.TEXT));
        case FUNCTION:
            return generateFunction(depth + 1, MaterializeDataType.TEXT);
        case CONCAT:
            return generateConcat(depth);
        default:
            throw new AssertionError();
        }
    }

    private MaterializeExpression generateConcat(int depth) {
        MaterializeExpression left = generateExpression(depth + 1, MaterializeDataType.TEXT);
        MaterializeExpression right = generateExpression(depth + 1);
        return new MaterializeConcatOperation(left, right);
    }

    private enum BitExpression {
        BINARY_OPERATION
    };

    private MaterializeExpression generateBitExpression(int depth) {
        BitExpression option;
        option = Randomly.fromOptions(BitExpression.values());
        switch (option) {
        case BINARY_OPERATION:
            return new MaterializeBinaryBitOperation(MaterializeBinaryBitOperator.getRandom(),
                    generateExpression(depth + 1, MaterializeDataType.BIT),
                    generateExpression(depth + 1, MaterializeDataType.BIT));
        default:
            throw new AssertionError();
        }
    }

    private enum IntExpression {
        UNARY_OPERATION, FUNCTION, /* CAST, */BINARY_ARITHMETIC_EXPRESSION
    }

    private MaterializeExpression generateIntExpression(int depth) {
        IntExpression option;
        option = Randomly.fromOptions(IntExpression.values());
        switch (option) {
        case UNARY_OPERATION:
            MaterializeExpression intExpression = generateExpression(depth + 1, MaterializeDataType.INT);
            return new MaterializePrefixOperation(intExpression,
                    Randomly.getBoolean() ? PrefixOperator.UNARY_PLUS : PrefixOperator.UNARY_MINUS);
        case FUNCTION:
            return generateFunction(depth + 1, MaterializeDataType.INT);
        case BINARY_ARITHMETIC_EXPRESSION:
            return new MaterializeBinaryArithmeticOperation(generateExpression(depth + 1, MaterializeDataType.INT),
                    generateExpression(depth + 1, MaterializeDataType.INT), MaterializeBinaryOperator.getRandom());
        default:
            throw new AssertionError();
        }
    }

    private MaterializeExpression createColumnOfType(MaterializeDataType type) {
        List<MaterializeColumn> columns = filterColumns(type);
        MaterializeColumn fromList = Randomly.fromList(columns);
        MaterializeConstant value = rw == null ? null : rw.getValues().get(fromList);
        return MaterializeColumnValue.create(fromList, value);
    }

    final List<MaterializeColumn> filterColumns(MaterializeDataType type) {
        if (columns == null) {
            return Collections.emptyList();
        } else {
            return columns.stream().filter(c -> c.getType() == type).collect(Collectors.toList());
        }
    }

    public MaterializeExpression generateExpressionWithExpectedResult(MaterializeDataType type) {
        this.expectedResult = true;
        MaterializeExpressionGenerator gen = new MaterializeExpressionGenerator(globalState).setColumns(columns)
                .setRowValue(rw);
        MaterializeExpression expr;
        do {
            expr = gen.generateExpression(type);
        } while (expr.getExpectedValue() == null);
        return expr;
    }

    public static MaterializeExpression generateConstant(Randomly r, MaterializeDataType type) {
        if (Randomly.getBooleanWithSmallProbability()) {
            return MaterializeConstant.createNullConstant();
        }
        switch (type) {
        case INT:
            if (Randomly.getBooleanWithSmallProbability()) {
                return MaterializeConstant.createTextConstant(String.valueOf(r.getInteger()));
            } else {
                return MaterializeConstant.createIntConstant(r.getInteger());
            }
        case BOOLEAN:
            if (Randomly.getBooleanWithSmallProbability() && !MaterializeProvider.generateOnlyKnown) {
                return MaterializeConstant
                        .createTextConstant(Randomly.fromOptions("TR", "TRUE", "FA", "FALSE", "0", "1", "ON", "off"));
            } else {
                return MaterializeConstant.createBooleanConstant(Randomly.getBoolean());
            }
        case TEXT:
            return MaterializeConstant.createTextConstant(r.getString());
        case DECIMAL:
            return MaterializeConstant.createDecimalConstant(r.getRandomBigDecimal());
        case FLOAT:
            return MaterializeConstant.createFloatConstant((float) r.getDouble());
        case REAL:
            return MaterializeConstant.createDoubleConstant(r.getDouble());
        case BIT:
            return MaterializeConstant.createBitConstant(r.getInteger());
        default:
            throw new AssertionError(type);
        }
    }

    public static MaterializeExpression generateExpression(MaterializeGlobalState globalState,
            List<MaterializeColumn> columns, MaterializeDataType type) {
        return new MaterializeExpressionGenerator(globalState).setColumns(columns).generateExpression(0, type);
    }

    public static MaterializeExpression generateExpression(MaterializeGlobalState globalState,
            List<MaterializeColumn> columns) {
        return new MaterializeExpressionGenerator(globalState).setColumns(columns).generateExpression(0);

    }

    public List<MaterializeExpression> generateExpressions(int nr) {
        List<MaterializeExpression> expressions = new ArrayList<>();
        for (int i = 0; i < nr; i++) {
            expressions.add(generateExpression(0));
        }
        return expressions;
    }

    public MaterializeExpression generateExpression(MaterializeDataType dataType) {
        return generateExpression(0, dataType);
    }

    public MaterializeExpressionGenerator setGlobalState(MaterializeGlobalState globalState) {
        this.globalState = globalState;
        return this;
    }

    public MaterializeExpression generateHavingClause() {
        this.allowAggregateFunctions = true;
        MaterializeExpression expression = generateExpression(MaterializeDataType.BOOLEAN);
        this.allowAggregateFunctions = false;
        return expression;
    }

    public MaterializeExpression generateAggregate() {
        return getAggregate(MaterializeDataType.getRandomType());
    }

    private MaterializeExpression getAggregate(MaterializeDataType dataType) {
        List<MaterializeAggregateFunction> aggregates = MaterializeAggregateFunction.getAggregates(dataType);
        MaterializeAggregateFunction agg = Randomly.fromList(aggregates);
        return generateArgsForAggregate(dataType, agg);
    }

    public MaterializeAggregate generateArgsForAggregate(MaterializeDataType dataType,
            MaterializeAggregateFunction agg) {
        List<MaterializeDataType> types = agg.getTypes(dataType);
        List<MaterializeExpression> args = new ArrayList<>();
        for (MaterializeDataType argType : types) {
            args.add(generateExpression(argType));
        }
        return new MaterializeAggregate(args, agg);
    }

    public MaterializeExpressionGenerator allowAggregates(boolean value) {
        allowAggregateFunctions = value;
        return this;
    }

    @Override
    public MaterializeExpression generatePredicate() {
        return generateExpression(MaterializeDataType.BOOLEAN);
    }

    @Override
    public MaterializeExpression negatePredicate(MaterializeExpression predicate) {
        return new MaterializePrefixOperation(predicate, MaterializePrefixOperation.PrefixOperator.NOT);
    }

    @Override
    public MaterializeExpression isNull(MaterializeExpression expr) {
        return new MaterializePostfixOperation(expr, PostfixOperator.IS_NULL);
    }

}
