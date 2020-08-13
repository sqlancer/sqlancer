package sqlancer.cockroachdb.gen;

import java.util.ArrayList;
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
import sqlancer.common.gen.TypedExpressionGenerator;

public class CockroachDBExpressionGenerator
        extends TypedExpressionGenerator<CockroachDBExpression, CockroachDBColumn, CockroachDBCompositeDataType> {

    private final CockroachDBGlobalState globalState;

    public CockroachDBExpressionGenerator(CockroachDBGlobalState globalState) {
        this.globalState = globalState;
    }

    @Override
    public CockroachDBExpression generateExpression(CockroachDBCompositeDataType dataType) {
        return generateExpression(dataType, 0);
    }

    public CockroachDBExpression generateAggregate() {
        return getAggregate(getRandomType());
    }

    public CockroachDBExpression generateHavingClause() {
        allowAggregates = true;
        CockroachDBExpression expression = generateExpression(CockroachDBDataType.BOOL.get());
        allowAggregates = false;
        return expression;
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

    @Override
    public CockroachDBExpression generateExpression(CockroachDBCompositeDataType type, int depth) {
        // if (type == CockroachDBDataType.FLOAT && Randomly.getBooleanWithRatherLowProbability()) {
        // type = CockroachDBDataType.INT;
        // }
        if (allowAggregates && Randomly.getBoolean()) {
            return getAggregate(type);
        }
        if (depth >= globalState.getOptions().getMaxExpressionDepth() || Randomly.getBoolean()) {
            return generateLeafNode(type);
        } else {
            if (Randomly.getBooleanWithRatherLowProbability()) {
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
                return new CockroachDBBinaryArithmeticOperation(
                        generateExpression(CockroachDBDataType.INT.get(), depth + 1),
                        generateExpression(CockroachDBDataType.INT.get(), depth + 1),
                        CockroachDBBinaryArithmeticOperator.getRandom());
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
            case ARRAY:
                return generateLeafNode(type); // TODO
            default:
                throw new AssertionError(type);
            }
        }
    }

    private CockroachDBExpression getAggregate(CockroachDBCompositeDataType type) {
        CockroachDBAggregateFunction agg = Randomly
                .fromList(CockroachDBAggregate.CockroachDBAggregateFunction.getAggregates(type.getPrimitiveDataType()));
        return generateArgsForAggregate(type, agg);
    }

    public CockroachDBAggregate generateArgsForAggregate(CockroachDBCompositeDataType type,
            CockroachDBAggregateFunction agg) {
        List<CockroachDBDataType> types = agg.getTypes(type.getPrimitiveDataType());
        List<CockroachDBExpression> args = new ArrayList<>();
        allowAggregates = false; //
        for (CockroachDBDataType argType : types) {
            args.add(generateExpression(argType.get()));
        }
        return new CockroachDBAggregate(agg, args);
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
            return new CockroachDBConcatOperation(generateExpression(CockroachDBDataType.STRING.get(), depth + 1),
                    generateExpression(CockroachDBDataType.STRING.get(), depth + 1));
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
            List<CockroachDBExpression> rightList = generateExpressions(type, Randomly.smallNumber() + 2, depth + 1);
            return new CockroachDBMultiValuedComparison(left, rightList, MultiValuedComparisonType.getRandom(),
                    MultiValuedComparisonOperator.getRandomGenericComparisonOperator());
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
        return new CockroachDBInOperation(generateExpression(type, depth + 1),
                generateExpressions(type, Randomly.smallNumber() + 1, depth + 1));
    }

    @Override
    protected CockroachDBCompositeDataType getRandomType() {
        if (columns.isEmpty() || Randomly.getBooleanWithRatherLowProbability()) {
            return CockroachDBCompositeDataType.getRandom();
        } else {
            return Randomly.fromList(columns).getType();
        }
    }

    private CockroachDBExpression getBinaryComparison(int depth) {
        CockroachDBCompositeDataType type = getRandomType();
        CockroachDBExpression left = generateExpression(type, depth + 1);
        CockroachDBExpression right = generateExpression(type, depth + 1);
        return new CockroachDBBinaryComparisonOperator(left, right, CockroachDBComparisonOperator.getRandom());
    }

    @Override
    protected boolean canGenerateColumnOfType(CockroachDBCompositeDataType type) {
        return columns.stream().anyMatch(c -> c.getType() == type);
    }

    @Override
    public CockroachDBExpression generateConstant(CockroachDBCompositeDataType type) {
        if (Randomly.getBooleanWithRatherLowProbability()) {
            return CockroachDBConstant.createNullConstant();
        }
        switch (type.getPrimitiveDataType()) {
        case INT:
        case SERIAL:
        case DECIMAL: // TODO: generate random decimals
            return CockroachDBConstant.createIntConstant(globalState.getRandomly().getInteger());
        case BOOL:
            return CockroachDBConstant.createBooleanConstant(Randomly.getBoolean());
        case STRING:
        case BYTES: // TODO: also generate byte constants
            return getStringConstant();
        case FLOAT:
            return CockroachDBConstant.createFloatConstant(globalState.getRandomly().getDouble());
        case BIT:
            return CockroachDBConstant.createBitConstantWithSize(type.getSize());
        case VARBIT:
            if (Randomly.getBoolean()) {
                return CockroachDBConstant.createBitConstant(globalState.getRandomly().getInteger());
            } else {
                return CockroachDBConstant.createBitConstantWithSize((int) Randomly.getNotCachedInteger(1, 10));
            }
        case INTERVAL:
            return CockroachDBConstant.createIntervalConstant(globalState.getRandomly().getInteger(),
                    globalState.getRandomly().getInteger(), globalState.getRandomly().getInteger(),
                    globalState.getRandomly().getInteger(), globalState.getRandomly().getInteger(),
                    globalState.getRandomly().getInteger());
        case TIMESTAMP:
            return CockroachDBConstant.createTimestampConstant(globalState.getRandomly().getInteger());
        case TIMESTAMPTZ:
            return CockroachDBConstant.createTimestamptzConstant(globalState.getRandomly().getInteger());
        case TIME:
            return CockroachDBConstant.createTimeConstant(globalState.getRandomly().getInteger());
        case TIMETZ:
            return CockroachDBConstant.createTimetz(globalState.getRandomly().getInteger());
        case ARRAY:
            List<CockroachDBExpression> elements = new ArrayList<>();
            for (int i = 0; i < Randomly.smallNumber(); i++) {
                elements.add(generateConstant(type.getElementType()));
            }
            return CockroachDBConstant.createArrayConstant(elements);
        case JSONB:
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

    @Override
    protected CockroachDBExpression generateColumn(CockroachDBCompositeDataType type) {
        CockroachDBColumn column = Randomly
                .fromList(columns.stream().filter(c -> c.getType() == type).collect(Collectors.toList()));
        CockroachDBExpression columnReference = new CockroachDBColumnReference(column);
        if (column.getType().isString() && Randomly.getBooleanWithRatherLowProbability()) {
            columnReference = new CockroachDBCollate(columnReference, CockroachDBCommon.getRandomCollate());
        }
        return columnReference;
    }

    @Override
    public CockroachDBExpression generatePredicate() {
        return generateExpression(CockroachDBDataType.BOOL.get());
    }

    @Override
    public CockroachDBExpression negatePredicate(CockroachDBExpression predicate) {
        return new CockroachDBNotOperation(predicate);
    }

    @Override
    public CockroachDBExpression isNull(CockroachDBExpression expr) {
        return new CockroachDBUnaryPostfixOperation(expr, CockroachDBUnaryPostfixOperator.IS_NULL);
    }

}
