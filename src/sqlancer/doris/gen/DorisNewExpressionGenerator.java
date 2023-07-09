package sqlancer.doris.gen;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.ast.newast.NewOrderingTerm;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.gen.TypedExpressionGenerator;
import sqlancer.doris.DorisProvider.DorisGlobalState;
import sqlancer.doris.DorisSchema.DorisColumn;
import sqlancer.doris.DorisSchema.DorisDataType;
import sqlancer.doris.DorisSchema.DorisRowValue;
import sqlancer.doris.ast.DorisAggregateOperation;
import sqlancer.doris.ast.DorisAggregateOperation.DorisAggregateFunction;
import sqlancer.doris.ast.DorisBetweenOperation;
import sqlancer.doris.ast.DorisBinaryArithmeticOperation;
import sqlancer.doris.ast.DorisBinaryArithmeticOperation.DorisBinaryArithmeticOperator;
import sqlancer.doris.ast.DorisBinaryComparisonOperation;
import sqlancer.doris.ast.DorisBinaryComparisonOperation.DorisBinaryComparisonOperator;
import sqlancer.doris.ast.DorisBinaryLogicalOperation;
import sqlancer.doris.ast.DorisBinaryLogicalOperation.DorisBinaryLogicalOperator;
import sqlancer.doris.ast.DorisCaseOperation;
import sqlancer.doris.ast.DorisCastOperation;
import sqlancer.doris.ast.DorisColumnValue;
import sqlancer.doris.ast.DorisConstant;
import sqlancer.doris.ast.DorisExpression;
import sqlancer.doris.ast.DorisFunctionOperation.DorisFunction;
import sqlancer.doris.ast.DorisInOperation;
import sqlancer.doris.ast.DorisLikeOperation;
import sqlancer.doris.ast.DorisOrderByTerm;
import sqlancer.doris.ast.DorisUnaryPostfixOperation;
import sqlancer.doris.ast.DorisUnaryPostfixOperation.DorisUnaryPostfixOperator;
import sqlancer.doris.ast.DorisUnaryPrefixOperation;
import sqlancer.doris.ast.DorisUnaryPrefixOperation.DorisUnaryPrefixOperator;
import sqlancer.doris.visitor.DorisExprToNode;

public class DorisNewExpressionGenerator extends TypedExpressionGenerator<DorisExpression, DorisColumn, DorisDataType> {

    private final DorisGlobalState globalState;

    private final int maxDepth;
    private boolean allowAggregateFunctions;
    private DorisRowValue rowValue;

    private Set<DorisColumnValue> columnOfLeafNode;

    public DorisNewExpressionGenerator setRowValue(DorisRowValue rowValue) {
        this.rowValue = rowValue;
        return this;
    }

    public void setColumnOfLeafNode(Set<DorisColumnValue> columnOfLeafNode) {
        this.columnOfLeafNode = columnOfLeafNode;
    }

    public DorisNewExpressionGenerator(DorisGlobalState globalState) {
        this.globalState = globalState;
        this.maxDepth = globalState.getOptions().getMaxExpressionDepth();
    }

    @Override
    public DorisExpression generateLeafNode(DorisDataType dataType) {
        if (Randomly.getBoolean()) {
            return generateConstant(dataType);
        } else {
            if (filterColumns(dataType).isEmpty()) {
                return generateConstant(dataType);
            } else {
                return createColumnOfType(dataType);
            }
        }
    }

    final List<DorisColumn> filterColumns(DorisDataType dataType) {
        if (columns == null) {
            return Collections.emptyList();
        } else {
            return columns.stream().filter(c -> c.getType().getPrimitiveDataType() == dataType)
                    .collect(Collectors.toList());
        }
    }

    private DorisExpression createColumnOfType(DorisDataType type) {
        List<DorisColumn> columns = filterColumns(type);
        DorisColumn column = Randomly.fromList(columns);
        DorisConstant value = rowValue == null ? null : rowValue.getValues().get(column);
        if (columnOfLeafNode != null) {
            columnOfLeafNode.add(DorisColumnValue.create(column, value));
        }
        return DorisColumnValue.create(column, value);
    }

    public List<Node<DorisExpression>> generateOrderBy() {
        List<DorisColumn> randomColumns = Randomly.subset(columns);
        return randomColumns.stream()
                .map(c -> new DorisOrderByTerm(new DorisColumnValue(c, null), NewOrderingTerm.Ordering.getRandom()))
                .collect(Collectors.toList());
    }

    @Override
    public DorisExpression generateExpression(DorisDataType type, int depth) {
        // todo: case operation should be add into generateExpression

        if (Randomly.getBooleanWithRatherLowProbability() || depth >= maxDepth) {
            return generateLeafNode(type);
        }

        if (globalState.getDbmsSpecificOptions().testFunctions && Randomly.getBooleanWithRatherLowProbability()) {
            List<DorisFunction> applicableFunctions = DorisFunction.getFunctionsCompatibleWith(type);
            if (!applicableFunctions.isEmpty()) {
                DorisFunction function = Randomly.fromList(applicableFunctions);
                return function.getCall(type, this, depth + 1);
            }
        }
        if (type != DorisDataType.NULL && globalState.getDbmsSpecificOptions().testCasts && Randomly.getBooleanWithRatherLowProbability()) {
            return new DorisCastOperation(DorisExprToNode.cast(generateExpression(getRandomType(), depth + 1)), type);
        }
        if (globalState.getDbmsSpecificOptions().testCase && Randomly.getBooleanWithRatherLowProbability()) {
            DorisExpression expr = generateExpression(DorisDataType.BOOLEAN, depth + 1);
            List<DorisExpression> conditions = new ArrayList<>();
            List<DorisExpression> cases = new ArrayList<>();
            for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
                conditions.add(generateExpression(DorisDataType.BOOLEAN, depth + 1));
                cases.add(generateExpression(type, depth + 1));
            }
            DorisExpression elseExpr = null;
            if (Randomly.getBoolean()) {
                elseExpr = generateExpression(type, depth + 1);
            }
            return new DorisCaseOperation(expr, conditions, cases, elseExpr);
        }

        switch (type) {
        case INT:
            return generateIntExpression(depth);
        case BOOLEAN:
            return generateBooleanExpression(depth);
        case FLOAT:
        case DECIMAL:
        case DATE:
        case DATETIME:
        case VARCHAR:
        case NULL:
            return generateConstant(type);
        default:
            throw new AssertionError();
        }
    }

    public List<DorisExpression> generateExpressions(int nr, DorisDataType type) {
        List<DorisExpression> expressions = new ArrayList<>();
        for (int i = 0; i < nr; i++) {
            expressions.add(generateExpression(type));
        }
        return expressions;
    }

    private enum IntExpression {
        UNARY_OPERATION, BINARY_ARITHMETIC_OPERATION
    }

    private DorisExpression generateIntExpression(int depth) {
        if (allowAggregateFunctions) {
            allowAggregateFunctions = false;
        }
        IntExpression intExpression = Randomly.fromOptions(IntExpression.values());
        switch (intExpression) {
        case UNARY_OPERATION:
            return new DorisUnaryPrefixOperation(generateExpression(DorisDataType.INT, depth + 1),
                    Randomly.getBoolean() ? DorisUnaryPrefixOperator.UNARY_PLUS : DorisUnaryPrefixOperator.UNARY_MINUS);
        case BINARY_ARITHMETIC_OPERATION:
            return new DorisBinaryArithmeticOperation(generateExpression(DorisDataType.INT, depth + 1),
                    generateExpression(DorisDataType.INT, depth + 1),
                    Randomly.fromOptions(DorisBinaryArithmeticOperator.values()));
        default:
            throw new AssertionError();
        }
    }

    private enum BooleanExpression {
        POSTFIX_OPERATOR, NOT, BINARY_LOGICAL_OPERATOR, BINARY_COMPARISON, LIKE, BETWEEN, IN_OPERATION;
        // SIMILAR_TO, POSIX_REGEX, BINARY_RANGE_COMPARISON,FUNCTION, CAST,;
    }

    DorisExpression generateBooleanExpression(int depth) {
        if (allowAggregateFunctions) {
            allowAggregateFunctions = false;
        }
        List<BooleanExpression> validOptions = new ArrayList<>(Arrays.asList(BooleanExpression.values()));
        if (!globalState.getDbmsSpecificOptions().testIn) {
            validOptions.remove(BooleanExpression.IN_OPERATION);
        }
        if (!globalState.getDbmsSpecificOptions().testBinaryLogicals) {
            validOptions.remove(BooleanExpression.BINARY_LOGICAL_OPERATOR);
        }
        if (!globalState.getDbmsSpecificOptions().testBinaryComparisons) {
            validOptions.remove(BooleanExpression.BINARY_COMPARISON);
        }
        if (!globalState.getDbmsSpecificOptions().testBetween) {
            validOptions.remove(BooleanExpression.BETWEEN);
        }

        BooleanExpression option = Randomly.fromList(validOptions);
        switch (option) {
        case POSTFIX_OPERATOR:
            return getPostfix(depth + 1);
        case NOT:
            return getNOT(depth + 1);
        case BETWEEN:
            return getBetween(depth + 1);
        case IN_OPERATION:
            return getIn(depth + 1);
        case BINARY_LOGICAL_OPERATOR:
            return getBinaryLogical(depth + 1, DorisDataType.BOOLEAN);
        case BINARY_COMPARISON:
            return getComparison(depth + 1);
        case LIKE:
            return getLike(depth + 1, DorisDataType.VARCHAR);
        default:
            throw new AssertionError();
        }

    }

    DorisExpression getPostfix(int depth) {
        DorisUnaryPostfixOperator randomOp = DorisUnaryPostfixOperator.getRandom();
        return new DorisUnaryPostfixOperation(
                generateExpression(Randomly.fromOptions(randomOp.getInputDataTypes()), depth), randomOp);
    }

    DorisExpression getNOT(int depth) {
        DorisUnaryPrefixOperator op = DorisUnaryPrefixOperator.NOT;
        return new DorisUnaryPrefixOperation(generateExpression(op.getRandomInputDataTypes(), depth), op);
    }

    DorisExpression getBetween(int depth) {
        DorisDataType dataType = Randomly.fromList(Arrays.asList(DorisDataType.values()).stream()
                .filter(t -> t != DorisDataType.BOOLEAN).collect(Collectors.toList()));

        return new DorisBetweenOperation(generateExpression(dataType, depth), generateExpression(dataType, depth),
                generateExpression(dataType, depth), Randomly.getBoolean());
    }

    DorisExpression getIn(int depth) {
        DorisDataType dataType = Randomly.fromOptions(DorisDataType.values());
        DorisExpression leftExpr = generateExpression(dataType, depth);
        List<DorisExpression> rightExprs = new ArrayList<>();
        int nr = Randomly.smallNumber() + 1;
        for (int i = 0; i < nr; i++) {
            rightExprs.add(generateExpression(dataType, depth));
        }
        return new DorisInOperation(leftExpr, rightExprs, Randomly.getBoolean());
    }

    DorisExpression getBinaryLogical(int depth, DorisDataType dataType) {
        DorisExpression expr = generateExpression(dataType, depth);
        int nr = Randomly.smallNumber() + 1;
        for (int i = 0; i < nr; i++) {
            expr = new DorisBinaryLogicalOperation(expr, generateExpression(DorisDataType.BOOLEAN, depth),
                    DorisBinaryLogicalOperator.getRandom());
        }
        return expr;
    }

    DorisExpression getComparison(int depth) {
        // 跳过boolean
        DorisDataType dataType = Randomly.fromList(Arrays.asList(DorisDataType.values()).stream()
                .filter(t -> t != DorisDataType.BOOLEAN).collect(Collectors.toList()));
        DorisExpression leftExpr = generateExpression(dataType, depth);
        DorisExpression rightExpr = generateExpression(dataType, depth);
        return new DorisBinaryComparisonOperation(leftExpr, rightExpr,
                Randomly.fromOptions(DorisBinaryComparisonOperator.values()));
    }

    DorisExpression getLike(int depth, DorisDataType dataType) {
        return new DorisLikeOperation(generateExpression(dataType, depth), generateExpression(dataType, depth),
                DorisLikeOperation.DorisLikeOperator.LIKE_OPERATOR);
    }

    public DorisExpression generateExpressionWithExpectedResult(DorisDataType type) {
        DorisExpression expr;
        do {
            expr = this.generateExpression(type);
        } while (expr.getExpectedValue() == null);
        return expr;
    }

    @Override
    public DorisExpression generatePredicate() {
        return generateExpression(DorisDataType.BOOLEAN);
    }

    @Override
    public DorisExpression negatePredicate(DorisExpression predicate) {
        return new DorisUnaryPrefixOperation(predicate, DorisUnaryPrefixOperator.NOT);
    }

    @Override
    public DorisExpression isNull(DorisExpression predicate) {
        return new DorisUnaryPostfixOperation(predicate, DorisUnaryPostfixOperator.IS_NULL);
    }

    public DorisExpression generateConstant(DorisDataType type, boolean isNullable) {
        if (!isNullable) {
            return createConstantWithoutNull(type);
        } else if (isNullable && Randomly.getBooleanWithSmallProbability()) {
            createConstant(DorisDataType.NULL);
        }
        return createConstant(type);
    }

    @Override
    public DorisExpression generateConstant(DorisDataType type) {
        if (Randomly.getBooleanWithSmallProbability()) {
            return DorisConstant.createNullConstant();
        }
        return createConstant(type);
    }

    public DorisExpression createConstantWithoutNull(DorisDataType type) {
        DorisExpression constant = createConstant(type);
        int loopCount = 0;
        while (constant instanceof DorisConstant.DorisNullConstant && loopCount < 1000) {
            constant = createConstant(type);
            loopCount++;
        }
        if (constant instanceof DorisConstant.DorisNullConstant) {
            throw new RuntimeException("Error occur in generate constantWithoutNull");
        }
        return constant;
    }

    public DorisExpression createConstant(DorisDataType type) {
        Randomly r = globalState.getRandomly();
        long timestamp;
        switch (type) {
        case INT:
            if (globalState.getDbmsSpecificOptions().testIntConstants) {
                return DorisConstant.createIntConstant(r.getInteger());
            }
            return DorisConstant.createNullConstant();
        case BOOLEAN:
            if (globalState.getDbmsSpecificOptions().testBooleanConstants) {
                return DorisConstant.createBooleanConstant(Randomly.getBoolean());
            }
            return DorisConstant.createNullConstant();
        case DECIMAL:
            if (globalState.getDbmsSpecificOptions().testDecimalConstants) {
                double v = r.getDouble();
                while (v == Double.MAX_VALUE || v == -Double.MAX_VALUE || v == Double.POSITIVE_INFINITY
                        || v == Double.NEGATIVE_INFINITY) {
                    v = r.getDouble();
                }

                // e.g. format 1234.413232532 to qualify num 34.4132
                String formatter = "%." + type.getDecimalScale() + "f";
                String vStr = String.format(formatter, v);
                int pointPos = vStr.indexOf('.');
                if (pointPos > type.getDecimalPrecision() - type.getDecimalScale()) {
                    vStr = vStr.substring(pointPos - (type.getDecimalPrecision() - type.getDecimalScale()));
                }
                return DorisConstant.createFloatConstant(Double.parseDouble(vStr));
            }
            return DorisConstant.createNullConstant();
        case FLOAT:
            if (globalState.getDbmsSpecificOptions().testFloatConstants) {
                return DorisConstant.createFloatConstant((float) r.getDouble());
            }
            return DorisConstant.createNullConstant();
        case DATE:
            if (globalState.getDbmsSpecificOptions().testDateConstants) {
                // [1970-01-01 08:00:00, 3000-01-01 00:00:00]
                timestamp = globalState.getRandomly().getLong(0, 32503651200L);
                return DorisConstant.createDateConstant(timestamp);
            }
            return DorisConstant.createNullConstant();
        case DATETIME:
            if (globalState.getDbmsSpecificOptions().testDateTimeConstants) {
                // [1970-01-01 08:00:00, 3000-01-01 00:00:00]
                timestamp = globalState.getRandomly().getLong(0, 32503651200L);
                return Randomly.fromOptions(DorisConstant.createDatetimeConstant(timestamp),
                        DorisConstant.createDatetimeConstant());
            }
            return DorisConstant.createNullConstant();
        case VARCHAR:
            if (globalState.getDbmsSpecificOptions().testStringConstants) {
                String s = r.getString();
                if (s.length() > type.getVarcharLength()) {
                    s = s.substring(0, type.getVarcharLength());
                }
                return DorisConstant.createStringConstant(s);
            }
            return DorisConstant.createNullConstant();
        case NULL:
            return DorisConstant.createNullConstant();
        default:
            throw new AssertionError(type);
        }
    }

    @Override
    protected DorisExpression generateColumn(DorisDataType type) {
        return null;
    }

    @Override
    protected DorisDataType getRandomType() {
        return Randomly.fromOptions(DorisDataType.values());
    }

    @Override
    protected boolean canGenerateColumnOfType(DorisDataType type) {
        return false;
    }

    public DorisExpression generateArgsForAggregate(DorisAggregateFunction aggregateFunction) {
        DorisDataType dataType = Randomly.fromOptions(DorisDataType.values());
        return new DorisAggregateOperation(generateExpressions(aggregateFunction.getNrArgs(), dataType),
                aggregateFunction);
    }

    public DorisExpression generateAggregate() {
        DorisAggregateFunction aggrFunc = DorisAggregateFunction.getRandom();
        return generateArgsForAggregate(aggrFunc);
    }

    public DorisExpression generateHavingClause() {
        allowAggregateFunctions = true;
        DorisExpression expression = generateExpression(DorisDataType.BOOLEAN);
        allowAggregateFunctions = false;
        return expression;
    }

    public void setAllowAggregateFunctions(boolean allowAggregateFunctions) {
        this.allowAggregateFunctions = allowAggregateFunctions;
    }
}
