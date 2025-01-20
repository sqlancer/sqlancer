package sqlancer.databend.gen;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.ast.newast.NewOrderingTerm;
import sqlancer.common.gen.NoRECGenerator;
import sqlancer.common.gen.TLPWhereGenerator;
import sqlancer.common.gen.TypedExpressionGenerator;
import sqlancer.common.schema.AbstractTables;
import sqlancer.databend.DatabendBugs;
import sqlancer.databend.DatabendProvider.DatabendGlobalState;
import sqlancer.databend.DatabendSchema.DatabendColumn;
import sqlancer.databend.DatabendSchema.DatabendCompositeDataType;
import sqlancer.databend.DatabendSchema.DatabendDataType;
import sqlancer.databend.DatabendSchema.DatabendRowValue;
import sqlancer.databend.DatabendSchema.DatabendTable;
import sqlancer.databend.DatabendToStringVisitor;
import sqlancer.databend.ast.DatabendAggregateOperation;
import sqlancer.databend.ast.DatabendAggregateOperation.DatabendAggregateFunction;
import sqlancer.databend.ast.DatabendBetweenOperation;
import sqlancer.databend.ast.DatabendBinaryArithmeticOperation;
import sqlancer.databend.ast.DatabendBinaryArithmeticOperation.DatabendBinaryArithmeticOperator;
import sqlancer.databend.ast.DatabendBinaryComparisonOperation;
import sqlancer.databend.ast.DatabendBinaryComparisonOperation.DatabendBinaryComparisonOperator;
import sqlancer.databend.ast.DatabendBinaryLogicalOperation;
import sqlancer.databend.ast.DatabendBinaryLogicalOperation.DatabendBinaryLogicalOperator;
import sqlancer.databend.ast.DatabendCastOperation;
import sqlancer.databend.ast.DatabendColumnReference;
import sqlancer.databend.ast.DatabendColumnValue;
import sqlancer.databend.ast.DatabendConstant;
import sqlancer.databend.ast.DatabendExpression;
import sqlancer.databend.ast.DatabendInOperation;
import sqlancer.databend.ast.DatabendJoin;
import sqlancer.databend.ast.DatabendLikeOperation;
import sqlancer.databend.ast.DatabendOrderByTerm;
import sqlancer.databend.ast.DatabendPostFixText;
import sqlancer.databend.ast.DatabendSelect;
import sqlancer.databend.ast.DatabendTableReference;
import sqlancer.databend.ast.DatabendUnaryPostfixOperation;
import sqlancer.databend.ast.DatabendUnaryPostfixOperation.DatabendUnaryPostfixOperator;
import sqlancer.databend.ast.DatabendUnaryPrefixOperation;
import sqlancer.databend.ast.DatabendUnaryPrefixOperation.DatabendUnaryPrefixOperator;

public class DatabendNewExpressionGenerator
        extends TypedExpressionGenerator<DatabendExpression, DatabendColumn, DatabendDataType>
        implements NoRECGenerator<DatabendSelect, DatabendJoin, DatabendExpression, DatabendTable, DatabendColumn>,
        TLPWhereGenerator<DatabendSelect, DatabendJoin, DatabendExpression, DatabendTable, DatabendColumn> {

    private final DatabendGlobalState globalState;
    private List<DatabendTable> tables;

    private final int maxDepth;
    private boolean allowAggregateFunctions;
    private DatabendRowValue rowValue;

    private Set<DatabendColumnValue> columnOfLeafNode;

    public DatabendNewExpressionGenerator setRowValue(DatabendRowValue rowValue) {
        this.rowValue = rowValue;
        return this;
    }

    public void setColumnOfLeafNode(Set<DatabendColumnValue> columnOfLeafNode) {
        this.columnOfLeafNode = columnOfLeafNode;
    }

    public DatabendNewExpressionGenerator(DatabendGlobalState globalState) {
        this.globalState = globalState;
        this.maxDepth = globalState.getOptions().getMaxExpressionDepth();
    }

    @Override
    public DatabendExpression generateLeafNode(DatabendDataType dataType) {
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

    final List<DatabendColumn> filterColumns(DatabendDataType dataType) {
        if (columns == null) {
            return Collections.emptyList();
        } else {
            return columns.stream().filter(c -> c.getType().getPrimitiveDataType() == dataType)
                    .collect(Collectors.toList());
        }
    }

    private DatabendExpression createColumnOfType(DatabendDataType type) {
        List<DatabendColumn> columns = filterColumns(type);
        DatabendColumn column = Randomly.fromList(columns);
        DatabendConstant value = rowValue == null ? null : rowValue.getValues().get(column);
        if (columnOfLeafNode != null) {
            columnOfLeafNode.add(DatabendColumnValue.create(column, value));
        }
        return DatabendColumnValue.create(column, value);
    }

    public List<DatabendExpression> generateOrderBy() {
        List<DatabendColumn> randomColumns = Randomly.subset(columns);
        return randomColumns.stream().map(
                c -> new DatabendOrderByTerm(new DatabendColumnValue(c, null), NewOrderingTerm.Ordering.getRandom()))
                .collect(Collectors.toList());
    }

    @Override
    protected DatabendExpression generateExpression(DatabendDataType type, int depth) {
        if (Randomly.getBooleanWithRatherLowProbability() || depth >= maxDepth) {
            return generateLeafNode(type);
        }

        switch (type) {
        case BOOLEAN:
            return generateBooleanExpression(depth);
        case INT:
            return generateIntExpression(depth);
        case FLOAT:
        case VARCHAR:
        case DATE:
        case TIMESTAMP:
        case NULL:
            return generateConstant(type);
        default:
            throw new AssertionError();
        }
    }

    public List<DatabendExpression> generateExpressions(int nr, DatabendDataType type) {
        List<DatabendExpression> expressions = new ArrayList<>();
        for (int i = 0; i < nr; i++) {
            expressions.add(generateExpression(type));
        }
        return expressions;
    }

    private enum IntExpression {
        UNARY_OPERATION, BINARY_ARITHMETIC_OPERATION
    }

    private DatabendExpression generateIntExpression(int depth) {
        if (allowAggregateFunctions) {
            allowAggregateFunctions = false;
        }
        IntExpression intExpression = Randomly.fromOptions(IntExpression.values());
        switch (intExpression) {
        case UNARY_OPERATION:
            return new DatabendUnaryPrefixOperation(generateExpression(DatabendDataType.INT, depth + 1),
                    Randomly.getBoolean() ? DatabendUnaryPrefixOperator.UNARY_PLUS
                            : DatabendUnaryPrefixOperator.UNARY_MINUS);
        case BINARY_ARITHMETIC_OPERATION:
            return new DatabendBinaryArithmeticOperation(generateExpression(DatabendDataType.INT, depth + 1),
                    generateExpression(DatabendDataType.INT, depth + 1),
                    Randomly.fromOptions(DatabendBinaryArithmeticOperator.values()));
        default:
            throw new AssertionError();
        }
    }

    private enum BooleanExpression {
        POSTFIX_OPERATOR, NOT, BINARY_LOGICAL_OPERATOR, BINARY_COMPARISON, LIKE, BETWEEN, IN_OPERATION;
        // SIMILAR_TO, POSIX_REGEX, BINARY_RANGE_COMPARISON,FUNCTION, CAST,;
    }

    DatabendExpression generateBooleanExpression(int depth) {
        if (allowAggregateFunctions) {
            allowAggregateFunctions = false;
        }
        List<BooleanExpression> validOptions = new ArrayList<>(Arrays.asList(BooleanExpression.values()));
        if (DatabendBugs.bug15570) {
            validOptions.remove(BooleanExpression.LIKE);
            validOptions.remove(BooleanExpression.IN_OPERATION);
            validOptions.remove(BooleanExpression.BETWEEN);
            validOptions.remove(BooleanExpression.BINARY_COMPARISON);
        }
        if (DatabendBugs.bug15572) {
            validOptions.remove(BooleanExpression.NOT);
        }
        BooleanExpression option = Randomly.fromList(validOptions);
        switch (option) {
        case POSTFIX_OPERATOR:
            return getPostfix(depth + 1);
        case NOT:
            return getNOT(depth + 1);
        case BETWEEN: // TODO (NULL BETWEEN NULL AND NULL) 返回的是 NULL 需要注意
            return getBetween(depth + 1);
        case IN_OPERATION:
            return getIn(depth + 1);
        case BINARY_LOGICAL_OPERATOR:
            return getBinaryLogical(depth + 1, DatabendDataType.BOOLEAN);
        case BINARY_COMPARISON:
            return getComparison(depth + 1);
        case LIKE:
            return getLike(depth + 1, DatabendDataType.VARCHAR);
        default:
            throw new AssertionError();
        }

    }

    DatabendExpression getPostfix(int depth) {
        DatabendUnaryPostfixOperator randomOp = DatabendUnaryPostfixOperator.getRandom();
        return new DatabendUnaryPostfixOperation(
                generateExpression(Randomly.fromOptions(randomOp.getInputDataTypes()), depth), randomOp);
    }

    DatabendExpression getNOT(int depth) {
        DatabendUnaryPrefixOperator op = DatabendUnaryPrefixOperator.NOT;
        return new DatabendUnaryPrefixOperation(generateExpression(op.getRandomInputDataTypes(), depth), op);
    }

    DatabendExpression getBetween(int depth) {
        // 跳过boolean
        DatabendDataType dataType = Randomly.fromList(Arrays.asList(DatabendDataType.values()).stream()
                .filter(t -> t != DatabendDataType.BOOLEAN).collect(Collectors.toList()));

        return new DatabendBetweenOperation(generateExpression(dataType, depth), generateExpression(dataType, depth),
                generateExpression(dataType, depth), Randomly.getBoolean());
    }

    DatabendExpression getIn(int depth) {
        DatabendDataType dataType = Randomly.fromOptions(DatabendDataType.values());
        DatabendExpression leftExpr = generateExpression(dataType, depth);
        List<DatabendExpression> rightExprs = new ArrayList<>();
        int nr = Randomly.smallNumber() + 1;
        for (int i = 0; i < nr; i++) {
            rightExprs.add(generateExpression(dataType, depth));
        }
        return new DatabendInOperation(leftExpr, rightExprs, Randomly.getBoolean());
    }

    DatabendExpression getBinaryLogical(int depth, DatabendDataType dataType) {
        DatabendExpression expr = generateExpression(dataType, depth);
        int nr = Randomly.smallNumber() + 1;
        for (int i = 0; i < nr; i++) {
            expr = new DatabendBinaryLogicalOperation(expr, generateExpression(DatabendDataType.BOOLEAN, depth),
                    DatabendBinaryLogicalOperator.getRandom());
        }
        return expr;
    }

    DatabendExpression getComparison(int depth) {
        // 跳过boolean
        DatabendDataType dataType = Randomly.fromList(Arrays.asList(DatabendDataType.values()).stream()
                .filter(t -> t != DatabendDataType.BOOLEAN).collect(Collectors.toList()));
        DatabendExpression leftExpr = generateExpression(dataType, depth);
        DatabendExpression rightExpr = generateExpression(dataType, depth);
        return new DatabendBinaryComparisonOperation(leftExpr, rightExpr,
                Randomly.fromOptions(DatabendBinaryComparisonOperator.values()));
    }

    DatabendExpression getLike(int depth, DatabendDataType dataType) {
        return new DatabendLikeOperation(generateExpression(dataType, depth), generateExpression(dataType, depth),
                DatabendLikeOperation.DatabendLikeOperator.LIKE_OPERATOR);
    }

    public DatabendExpression generateExpressionWithExpectedResult(DatabendDataType type) {
        // DatabendNewExpressionGenerator gen = new
        // DatabendNewExpressionGenerator(globalState).setColumns(columns);
        // gen.setRowValue(rowValue);
        DatabendExpression expr;
        do {
            expr = this.generateExpression(type);
        } while (expr.getExpectedValue() == null);
        return expr;
    }

    @Override
    public DatabendExpression generatePredicate() {
        return generateExpression(DatabendDataType.BOOLEAN);
    }

    @Override
    public DatabendExpression negatePredicate(DatabendExpression predicate) {
        return new DatabendUnaryPrefixOperation(predicate, DatabendUnaryPrefixOperator.NOT);
    }

    @Override
    public DatabendExpression isNull(DatabendExpression predicate) {
        return new DatabendUnaryPostfixOperation(predicate, DatabendUnaryPostfixOperator.IS_NULL);
    }

    public DatabendExpression generateConstant(DatabendDataType type, boolean isNullable) {
        if (isNullable && Randomly.getBooleanWithSmallProbability()) {
            createConstant(DatabendDataType.NULL);
        }
        return createConstant(type);
    }

    @Override
    public DatabendExpression generateConstant(DatabendDataType type) {
        if (Randomly.getBooleanWithSmallProbability()) {
            return DatabendConstant.createNullConstant();
        }
        return createConstant(type);
    }

    public DatabendExpression createConstant(DatabendDataType type) {
        Randomly r = globalState.getRandomly();
        switch (type) {
        case INT:
            // TODO 已支持数值型string转化但仍然不支持运算符计算，待添加
            return DatabendConstant.createIntConstant(r.getInteger());
        case BOOLEAN:
            // TODO 已支持boolean型string转化但仍然不支持运算符计算，待添加
            return DatabendConstant.createBooleanConstant(Randomly.getBoolean());
        case FLOAT:
            return DatabendConstant.createFloatConstant((float) r.getDouble());
        case VARCHAR:
            return DatabendConstant.createStringConstant(r.getString());
        case NULL:
            return DatabendConstant.createNullConstant();
        case DATE:
            return DatabendConstant.createDateConstant(r.getInteger());
        case TIMESTAMP:
            return DatabendConstant.createTimestampConstant(r.getInteger());
        default:
            throw new AssertionError(type);
        }
    }

    @Override
    protected DatabendExpression generateColumn(DatabendDataType type) {
        return null;
    }

    @Override
    protected DatabendDataType getRandomType() {
        return Randomly.fromOptions(DatabendDataType.values());
    }

    @Override
    protected boolean canGenerateColumnOfType(DatabendDataType type) {
        return false;
    }

    public DatabendExpression generateArgsForAggregate(DatabendAggregateFunction aggregateFunction) {
        return new DatabendAggregateOperation(
                generateExpressions(aggregateFunction.getNrArgs(), aggregateFunction.getRandomType()),
                aggregateFunction);
    }

    public DatabendExpression generateAggregate() {
        DatabendAggregateFunction aggrFunc = DatabendAggregateFunction.getRandom();
        return generateArgsForAggregate(aggrFunc);
    }

    public DatabendExpression generateHavingClause() {
        allowAggregateFunctions = true;
        DatabendExpression expression = generateExpression(DatabendDataType.BOOLEAN);
        allowAggregateFunctions = false;
        return expression;
    }

    @Override
    public DatabendNewExpressionGenerator setTablesAndColumns(AbstractTables<DatabendTable, DatabendColumn> tables) {
        this.columns = tables.getColumns();
        this.tables = tables.getTables();

        return this;
    }

    @Override
    public DatabendExpression generateBooleanExpression() {
        return generateExpression(DatabendDataType.BOOLEAN);
    }

    @Override
    public DatabendSelect generateSelect() {
        return new DatabendSelect();
    }

    @Override
    public List<DatabendJoin> getRandomJoinClauses() {
        List<DatabendTableReference> tableList = tables.stream().map(t -> new DatabendTableReference(t))
                .collect(Collectors.toList());
        List<DatabendJoin> joins = DatabendJoin.getJoins(tableList, globalState);
        tables = tableList.stream().map(t -> t.getTable()).collect(Collectors.toList());
        return joins;
    }

    @Override
    public List<DatabendExpression> getTableRefs() {
        return tables.stream().map(t -> new DatabendTableReference(t)).collect(Collectors.toList());
    }

    @Override
    public String generateOptimizedQueryString(DatabendSelect select, DatabendExpression whereCondition,
            boolean shouldUseAggregate) {
        if (shouldUseAggregate) {
            DatabendExpression aggr = new DatabendAggregateOperation(
                    List.of(new DatabendColumnReference(new DatabendColumn("*",
                            new DatabendCompositeDataType(DatabendDataType.INT, 0), false, false))),
                    DatabendAggregateFunction.COUNT);
            select.setFetchColumns(List.of(aggr));
        } else {
            List<DatabendExpression> allColumns = columns.stream().map((c) -> new DatabendColumnReference(c))
                    .collect(Collectors.toList());
            select.setFetchColumns(allColumns);
            if (Randomly.getBooleanWithSmallProbability()) {
                select.setOrderByClauses(generateOrderBys());
            }
        }
        select.setWhereClause(whereCondition);

        return select.asString();
    }

    @Override
    public String generateUnoptimizedQueryString(DatabendSelect select, DatabendExpression whereCondition) {
        DatabendExpression asText = new DatabendPostFixText(new DatabendCastOperation(
                new DatabendPostFixText(whereCondition,
                        " IS NOT NULL AND " + DatabendToStringVisitor.asString(whereCondition)),
                new DatabendCompositeDataType(DatabendDataType.INT, 8)), "as count");
        select.setFetchColumns(List.of(asText));
        select.setWhereClause(null);

        return "SELECT SUM(count) FROM (" + select.asString() + ") as res";
    }

    @Override
    public List<DatabendExpression> generateFetchColumns(boolean shouldCreateDummy) {
        if (shouldCreateDummy) {
            return List.of(new DatabendColumnReference(new DatabendColumn("*", null, false, false)));
        }
        return columns.stream().map(c -> new DatabendColumnReference(c)).collect(Collectors.toList());
    }
}
