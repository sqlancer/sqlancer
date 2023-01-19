package sqlancer.cnosdb.gen;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.cnosdb.CnosDBCompoundDataType;
import sqlancer.cnosdb.CnosDBGlobalState;
import sqlancer.cnosdb.CnosDBSchema.CnosDBColumn;
import sqlancer.cnosdb.CnosDBSchema.CnosDBDataType;
import sqlancer.cnosdb.CnosDBSchema.CnosDBRowValue;
import sqlancer.cnosdb.ast.CnosDBAggregate;
import sqlancer.cnosdb.ast.CnosDBAggregate.CnosDBAggregateFunction;
import sqlancer.cnosdb.ast.CnosDBBetweenOperation;
import sqlancer.cnosdb.ast.CnosDBBinaryArithmeticOperation;
import sqlancer.cnosdb.ast.CnosDBBinaryArithmeticOperation.CnosDBBinaryOperator;
import sqlancer.cnosdb.ast.CnosDBBinaryComparisonOperation;
import sqlancer.cnosdb.ast.CnosDBBinaryLogicalOperation;
import sqlancer.cnosdb.ast.CnosDBBinaryLogicalOperation.BinaryLogicalOperator;
import sqlancer.cnosdb.ast.CnosDBCastOperation;
import sqlancer.cnosdb.ast.CnosDBColumnValue;
import sqlancer.cnosdb.ast.CnosDBConcatOperation;
import sqlancer.cnosdb.ast.CnosDBConstant;
import sqlancer.cnosdb.ast.CnosDBExpression;
import sqlancer.cnosdb.ast.CnosDBFunction;
import sqlancer.cnosdb.ast.CnosDBFunction.CnosDBFunctionWithResult;
import sqlancer.cnosdb.ast.CnosDBFunctionWithUnknownResult;
import sqlancer.cnosdb.ast.CnosDBInOperation;
import sqlancer.cnosdb.ast.CnosDBLikeOperation;
import sqlancer.cnosdb.ast.CnosDBOrderByTerm;
import sqlancer.cnosdb.ast.CnosDBOrderByTerm.CnosDBOrder;
import sqlancer.cnosdb.ast.CnosDBPostfixOperation;
import sqlancer.cnosdb.ast.CnosDBPostfixOperation.PostfixOperator;
import sqlancer.cnosdb.ast.CnosDBPrefixOperation;
import sqlancer.cnosdb.ast.CnosDBPrefixOperation.PrefixOperator;
import sqlancer.cnosdb.ast.CnosDBSimilarTo;
import sqlancer.common.gen.ExpressionGenerator;

public class CnosDBExpressionGenerator implements ExpressionGenerator<CnosDBExpression> {

    private final int maxDepth;

    private final Randomly r;

    private List<CnosDBColumn> columns;

    private CnosDBRowValue rw;

    private CnosDBGlobalState globalState;

    private boolean allowAggregateFunctions;

    public CnosDBExpressionGenerator(CnosDBGlobalState globalState) {
        this.r = globalState.getRandomly();
        this.maxDepth = globalState.getOptions().getMaxExpressionDepth();
        this.globalState = globalState;
    }

    public static CnosDBExpression generateExpression(CnosDBGlobalState globalState, CnosDBDataType type) {
        return new CnosDBExpressionGenerator(globalState).generateExpression(0, type);
    }

    private static CnosDBCompoundDataType getCompoundDataType(CnosDBDataType type) {
        return CnosDBCompoundDataType.create(type);
    }

    public static CnosDBExpression generateConstant(Randomly r, CnosDBDataType type) {
        if (Randomly.getBooleanWithSmallProbability()) {
            return CnosDBConstant.createNullConstant();
        }
        switch (type) {
        case INT:
            return CnosDBConstant.createIntConstant(r.getInteger());
        case UINT:
            return CnosDBConstant.createUintConstant(r.getPositiveInteger());
        case TIMESTAMP:
            return CnosDBConstant.createTimeStampConstant(r.getPositiveIntegerNotNull());
        case BOOLEAN:
            return CnosDBConstant.createBooleanConstant(Randomly.getBoolean());
        case STRING:
            return CnosDBConstant.createStringConstant(r.getString());
        case DOUBLE:
            return CnosDBConstant.createDoubleConstant(r.getDouble());
        default:
            throw new AssertionError(type);
        }
    }

    public static CnosDBExpression generateExpression(CnosDBGlobalState globalState, List<CnosDBColumn> columns,
            CnosDBDataType type) {
        return new CnosDBExpressionGenerator(globalState).setColumns(columns).generateExpression(0, type);
    }

    public static CnosDBExpression generateExpression(CnosDBGlobalState globalState, List<CnosDBColumn> columns) {
        return new CnosDBExpressionGenerator(globalState).setColumns(columns).generateExpression(0);
    }

    public CnosDBExpressionGenerator setColumns(List<CnosDBColumn> columns) {
        this.columns = columns;
        return this;
    }

    public CnosDBExpressionGenerator setRowValue(CnosDBRowValue rw) {
        this.rw = rw;
        return this;
    }

    public CnosDBExpression generateExpression(int depth) {
        return generateExpression(depth, CnosDBDataType.getRandomType());
    }

    public List<CnosDBExpression> generateOrderBy() {
        List<CnosDBExpression> orderBys = new ArrayList<>();
        for (int i = 0; i < Randomly.smallNumber(); i++) {
            orderBys.add(new CnosDBOrderByTerm(CnosDBColumnValue.create(Randomly.fromList(columns), null),
                    CnosDBOrder.getRandomOrder()));
        }
        return orderBys;
    }

    private CnosDBExpression generateFunctionWithUnknownResult(int depth, CnosDBDataType type) {
        List<CnosDBFunctionWithUnknownResult> supportedFunctions = CnosDBFunctionWithUnknownResult
                .getSupportedFunctions(type);
        if (supportedFunctions.isEmpty()) {
            throw new IgnoreMeException();
        }
        CnosDBFunctionWithUnknownResult randomFunction = Randomly.fromList(supportedFunctions);
        return new CnosDBFunction(randomFunction, type, randomFunction.getArguments(type, this, depth + 1));
    }

    private CnosDBExpression generateFunctionWithKnownResult(int depth, CnosDBDataType type) {
        List<CnosDBFunctionWithResult> functions = Stream.of(CnosDBFunctionWithResult.values())
                .filter(f -> f.supportsReturnType(type)).collect(Collectors.toList());
        if (functions.isEmpty()) {
            throw new IgnoreMeException();
        }
        CnosDBFunctionWithResult randomFunction = Randomly.fromList(functions);
        int nrArgs = randomFunction.getNrArgs();
        if (randomFunction.isVariadic()) {
            nrArgs += Randomly.smallNumber();
        }
        CnosDBDataType[] argTypes = randomFunction.getInputTypesForReturnType(type, nrArgs);
        CnosDBExpression[] args = new CnosDBExpression[nrArgs];
        do {
            for (int i = 0; i < args.length; i++) {
                args[i] = generateExpression(depth + 1, argTypes[i]);
            }
        } while (!randomFunction.checkArguments(args));
        return new CnosDBFunction(randomFunction, type, args);
    }

    private CnosDBExpression generateBooleanExpression(int depth) {
        List<BooleanExpression> validOptions = new ArrayList<>(Arrays.asList(BooleanExpression.values()));
        BooleanExpression option = Randomly.fromList(validOptions);
        switch (option) {
        case POSTFIX_OPERATOR:
            PostfixOperator random = PostfixOperator.getRandom();
            return CnosDBPostfixOperation
                    .create(generateExpression(depth + 1, Randomly.fromOptions(random.getInputDataTypes())), random);
        case IN_OPERATION:
            return inOperation(depth + 1);
        case NOT:
            return new CnosDBPrefixOperation(generateExpression(depth + 1, CnosDBDataType.BOOLEAN), PrefixOperator.NOT);
        case BINARY_LOGICAL_OPERATOR:
            CnosDBExpression first = generateExpression(depth + 1, CnosDBDataType.BOOLEAN);
            int nr = Randomly.smallNumber() + 1;
            for (int i = 0; i < nr; i++) {
                first = new CnosDBBinaryLogicalOperation(first, generateExpression(depth + 1, CnosDBDataType.BOOLEAN),
                        BinaryLogicalOperator.getRandom());
            }
            return first;
        case BINARY_COMPARISON:
            CnosDBDataType dataType = getMeaningfulType();
            return generateComparison(depth, dataType);
        case CAST:
            return generateCastExpression(depth + 1, CnosDBDataType.BOOLEAN);
        case FUNCTION:
            return generateFunction(depth + 1, CnosDBDataType.BOOLEAN);
        case LIKE:
            return new CnosDBLikeOperation(generateExpression(depth + 1, CnosDBDataType.STRING),
                    generateExpression(depth + 1, CnosDBDataType.STRING));
        case BETWEEN:
            CnosDBDataType type = getMeaningfulType();
            return new CnosDBBetweenOperation(generateExpression(depth + 1, type), generateExpression(depth + 1, type),
                    generateExpression(depth + 1, type));
        case SIMILAR_TO:
            return new CnosDBSimilarTo(generateExpression(depth + 1, CnosDBDataType.STRING),
                    generateExpression(depth + 1, CnosDBDataType.STRING), null);
        default:
            throw new AssertionError();
        }
    }

    private CnosDBDataType getMeaningfulType() {
        // make it more likely that the expression does not only consist of constant
        // expressions
        if (Randomly.getBooleanWithSmallProbability() || columns == null || columns.isEmpty()) {
            return CnosDBDataType.getRandomType();
        } else {
            return Randomly.fromList(columns).getType();
        }
    }

    private CnosDBExpression generateFunction(int depth, CnosDBDataType type) {
        if (Randomly.getBoolean()) {
            return generateFunctionWithKnownResult(depth, type);
        } else {
            return generateFunctionWithUnknownResult(depth, type);
        }
    }

    private CnosDBExpression generateComparison(int depth, CnosDBDataType dataType) {
        CnosDBExpression leftExpr = generateExpression(depth + 1, dataType);
        CnosDBExpression rightExpr = generateExpression(depth + 1, dataType);
        return getComparison(leftExpr, rightExpr);
    }

    private CnosDBExpression getComparison(CnosDBExpression leftExpr, CnosDBExpression rightExpr) {
        return new CnosDBBinaryComparisonOperation(leftExpr, rightExpr,
                CnosDBBinaryComparisonOperation.CnosDBBinaryComparisonOperator.getRandom());
    }

    private CnosDBExpression inOperation(int depth) {
        CnosDBDataType type = CnosDBDataType.getRandomType();
        CnosDBExpression leftExpr = generateExpression(depth + 1, type);
        List<CnosDBExpression> rightExpr = new ArrayList<>();
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
            rightExpr.add(generateConstant(new Randomly(), type));
        }
        return new CnosDBInOperation(leftExpr, rightExpr, Randomly.getBoolean());
    }

    public CnosDBExpression generateExpression(int depth, CnosDBDataType originalType) {
        return generateExpressionInternal(depth, originalType);
    }

    private CnosDBExpression generateExpressionInternal(int depth, CnosDBDataType dataType) throws AssertionError {
        if (allowAggregateFunctions && Randomly.getBoolean()) {
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
                    return generateCastExpression(depth + 1, dataType);
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
            case UINT:
                return generateUIntExpression(depth);
            case STRING:
                return generateStringExpression(depth);
            case DOUBLE:
                return generateFloatExpression(depth);
            case TIMESTAMP:
                return generateTimeStampExpression(depth);
            default:
                throw new AssertionError(dataType);
            }
        }
    }

    private CnosDBExpression generateStringExpression(int depth) {
        StringExpression option;
        List<StringExpression> validOptions = new ArrayList<>(Arrays.asList(StringExpression.values()));
        option = Randomly.fromList(validOptions);

        switch (option) {
        case CAST:
            return generateCastExpression(depth + 1, CnosDBDataType.STRING);
        case FUNCTION:
            return generateFunction(depth + 1, CnosDBDataType.STRING);
        case CONCAT:
            return generateConcat(depth);
        default:
            throw new AssertionError();
        }
    }

    private CnosDBExpression generateConcat(int depth) {
        CnosDBExpression left = generateExpression(depth + 1, CnosDBDataType.STRING);
        CnosDBExpression right = generateExpression(depth + 1);
        return new CnosDBConcatOperation(left, right);
    }

    private CnosDBExpression generateIntExpression(int depth) {
        IntExpression option;
        option = Randomly.fromOptions(IntExpression.values());
        switch (option) {
        case CAST:
            return generateCastExpression(depth + 1, CnosDBDataType.INT);
        case UNARY_OPERATION:
            CnosDBExpression intExpression = generateExpression(depth + 1, CnosDBDataType.INT);
            return new CnosDBPrefixOperation(intExpression,
                    Randomly.getBoolean() ? PrefixOperator.UNARY_PLUS : PrefixOperator.UNARY_MINUS);
        case FUNCTION:
            return generateFunction(depth + 1, CnosDBDataType.INT);
        case BINARY_ARITHMETIC_EXPRESSION:
            return new CnosDBBinaryArithmeticOperation(generateExpression(depth + 1, CnosDBDataType.INT),
                    generateExpression(depth + 1, CnosDBDataType.INT),
                    CnosDBBinaryOperator.getRandom(CnosDBDataType.INT));
        default:
            throw new AssertionError();
        }
    }

    private CnosDBExpression generateUIntExpression(int depth) {
        UIntExpression option = Randomly.fromOptions(UIntExpression.values());
        switch (option) {
        case CAST:
            return generateCastExpression(depth + 1, CnosDBDataType.UINT);
        case FUNCTION:
            return generateFunction(depth + 1, CnosDBDataType.UINT);
        case BINARY_ARITHMETIC_EXPRESSION:
            return new CnosDBBinaryArithmeticOperation(generateExpression(depth + 1, CnosDBDataType.UINT),
                    generateExpression(depth + 1, CnosDBDataType.UINT),
                    CnosDBBinaryOperator.getRandom(CnosDBDataType.UINT));
        default:
            throw new AssertionError();
        }

    }

    private CnosDBExpression generateFloatExpression(int depth) {
        FloatExpression option;
        option = Randomly.fromOptions(FloatExpression.values());
        switch (option) {
        case CAST:
            return generateCastExpression(depth + 1, CnosDBDataType.DOUBLE);
        case UNARY_OPERATION:
            CnosDBExpression floatExpression = generateExpression(depth + 1, CnosDBDataType.DOUBLE);
            return new CnosDBPrefixOperation(floatExpression,
                    Randomly.getBoolean() ? PrefixOperator.UNARY_PLUS : PrefixOperator.UNARY_MINUS);
        case FUNCTION:
            return generateFunction(depth + 1, CnosDBDataType.DOUBLE);
        case BINARY_ARITHMETIC_EXPRESSION:
            return new CnosDBBinaryArithmeticOperation(generateExpression(depth + 1, CnosDBDataType.DOUBLE),
                    generateExpression(depth + 1, CnosDBDataType.DOUBLE),
                    CnosDBBinaryOperator.getRandom(CnosDBDataType.DOUBLE));
        case CONSTANT:
            return generateConstant(r, CnosDBDataType.DOUBLE);
        default:
            throw new AssertionError();
        }
    }

    private CnosDBExpression generateTimeStampExpression(int depth) {
        if (Randomly.getBoolean()) {
            return generateConstant(r, CnosDBDataType.TIMESTAMP);
        }
        TimestampExpression option;
        option = Randomly.fromOptions(TimestampExpression.values());
        switch (option) {
        case CAST:
            return generateCastExpression(depth + 1, CnosDBDataType.TIMESTAMP);
        case FUNCTION:
            return generateFunction(depth + 1, CnosDBDataType.TIMESTAMP);
        default:
            throw new AssertionError();
        }
    }

    private CnosDBExpression generateCastExpression(int depth, CnosDBDataType castToType) {
        CnosDBDataType castFromType = Randomly.fromList(CnosDBCastOperation.canCastTo(castToType));
        return new CnosDBCastOperation(generateExpression(depth + 1, castFromType), getCompoundDataType(castToType));
    }

    private CnosDBExpression createColumnOfType(CnosDBDataType type) {
        List<CnosDBColumn> columns = filterColumns(type);
        if (columns.isEmpty()) {
            throw new IgnoreMeException();
        }
        CnosDBColumn fromList = Randomly.fromList(columns);
        CnosDBConstant value = rw == null ? null : rw.getValues().get(fromList);
        return CnosDBColumnValue.create(fromList, value);
    }

    final List<CnosDBColumn> filterColumns(CnosDBDataType type) {
        if (columns == null) {
            return Collections.emptyList();
        } else {
            return columns.stream().filter(c -> c.getType() == type).collect(Collectors.toList());
        }
    }

    public CnosDBExpression generateExpressionWithExpectedResult(CnosDBDataType type) {
        CnosDBExpressionGenerator gen = new CnosDBExpressionGenerator(globalState).setColumns(columns).setRowValue(rw);
        CnosDBExpression expr;
        do {
            expr = gen.generateExpression(type);
        } while (expr.getExpectedValue() == null);
        return expr;
    }

    public List<CnosDBExpression> generateExpressions(int nr) {
        List<CnosDBExpression> expressions = new ArrayList<>();
        for (int i = 0; i < nr; i++) {
            expressions.add(generateExpression(0));
        }
        return expressions;
    }

    public CnosDBExpression generateExpression(CnosDBDataType dataType) {
        return generateExpression(0, dataType);
    }

    public CnosDBExpressionGenerator setGlobalState(CnosDBGlobalState globalState) {
        this.globalState = globalState;
        return this;
    }

    public CnosDBExpression generateHavingClause() {
        this.allowAggregateFunctions = true;
        CnosDBExpression expression = generateExpression(CnosDBDataType.BOOLEAN);
        this.allowAggregateFunctions = false;
        return expression;
    }

    public CnosDBExpression generateAggregate() {
        return getAggregate(CnosDBDataType.getRandomType());
    }

    private CnosDBExpression getAggregate(CnosDBDataType dataType) {
        if (dataType == CnosDBDataType.BOOLEAN) {
            List<CnosDBAggregateFunction> aggregates = CnosDBAggregateFunction.getAggregates(CnosDBDataType.INT);
            CnosDBAggregateFunction agg = Randomly.fromList(aggregates);
            return new CnosDBCastOperation(generateArgsForAggregate(dataType, agg),
                    CnosDBCompoundDataType.create(CnosDBDataType.BOOLEAN));
        } else {
            List<CnosDBAggregateFunction> aggregates = CnosDBAggregateFunction.getAggregates(dataType);
            CnosDBAggregateFunction agg = Randomly.fromList(aggregates);
            return generateArgsForAggregate(dataType, agg);
        }
    }

    public CnosDBAggregate generateArgsForAggregate(CnosDBDataType dataType, CnosDBAggregateFunction agg) {
        CnosDBDataType[] types = agg.getInputTypes(dataType);
        List<CnosDBExpression> args = new ArrayList<>();
        for (CnosDBDataType argType : types) {
            args.add(createColumnOfType(argType));
            // args.add(generateExpression(argType));
        }
        return new CnosDBAggregate(args, agg);
    }

    public CnosDBExpressionGenerator allowAggregates(boolean value) {
        allowAggregateFunctions = value;
        return this;
    }

    @Override
    public CnosDBExpression generatePredicate() {
        return generateExpression(CnosDBDataType.BOOLEAN);
    }

    @Override
    public CnosDBExpression negatePredicate(CnosDBExpression predicate) {
        return new CnosDBPrefixOperation(predicate, PrefixOperator.NOT);
    }

    @Override
    public CnosDBExpression isNull(CnosDBExpression expr) {
        return new CnosDBPostfixOperation(expr, PostfixOperator.IS_NULL);
    }

    private enum BooleanExpression {
        POSTFIX_OPERATOR, NOT, BINARY_LOGICAL_OPERATOR, BINARY_COMPARISON, FUNCTION, CAST, LIKE, BETWEEN, IN_OPERATION,
        SIMILAR_TO,
    }

    private enum StringExpression {
        CAST, FUNCTION, CONCAT
    }

    private enum IntExpression {
        UNARY_OPERATION, FUNCTION, CAST, BINARY_ARITHMETIC_EXPRESSION
    }

    private enum UIntExpression {
        FUNCTION, CAST, BINARY_ARITHMETIC_EXPRESSION
    }

    private enum FloatExpression {
        UNARY_OPERATION, FUNCTION, CAST, BINARY_ARITHMETIC_EXPRESSION, CONSTANT
    }

    private enum TimestampExpression {
        FUNCTION, CAST
    }

}
