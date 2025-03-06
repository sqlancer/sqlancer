package sqlancer.clickhouse.gen;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.clickhouse.client.ClickHouseDataType;

import sqlancer.Randomly;
import sqlancer.clickhouse.ClickHouseProvider.ClickHouseGlobalState;
import sqlancer.clickhouse.ClickHouseSchema;
import sqlancer.clickhouse.ClickHouseSchema.ClickHouseColumn;
import sqlancer.clickhouse.ClickHouseSchema.ClickHouseLancerDataType;
import sqlancer.clickhouse.ClickHouseSchema.ClickHouseTable;
import sqlancer.clickhouse.ast.ClickHouseAggregate;
import sqlancer.clickhouse.ast.ClickHouseAggregate.ClickHouseAggregateFunction;
import sqlancer.clickhouse.ast.ClickHouseAliasOperation;
import sqlancer.clickhouse.ast.ClickHouseBinaryArithmeticOperation;
import sqlancer.clickhouse.ast.ClickHouseBinaryComparisonOperation;
import sqlancer.clickhouse.ast.ClickHouseBinaryFunctionOperation;
import sqlancer.clickhouse.ast.ClickHouseBinaryLogicalOperation;
import sqlancer.clickhouse.ast.ClickHouseColumnReference;
import sqlancer.clickhouse.ast.ClickHouseExpression;
import sqlancer.clickhouse.ast.ClickHouseExpression.ClickHouseJoin;
import sqlancer.clickhouse.ast.ClickHouseSelect;
import sqlancer.clickhouse.ast.ClickHouseTableReference;
import sqlancer.clickhouse.ast.ClickHouseUnaryFunctionOperation;
import sqlancer.clickhouse.ast.ClickHouseUnaryPostfixOperation;
import sqlancer.clickhouse.ast.ClickHouseUnaryPostfixOperation.ClickHouseUnaryPostfixOperator;
import sqlancer.clickhouse.ast.ClickHouseUnaryPrefixOperation;
import sqlancer.clickhouse.ast.ClickHouseUnaryPrefixOperation.ClickHouseUnaryPrefixOperator;
import sqlancer.clickhouse.ast.constant.ClickHouseCreateConstant;
import sqlancer.common.gen.NoRECGenerator;
import sqlancer.common.gen.TLPWhereGenerator;
import sqlancer.common.gen.TypedExpressionGenerator;
import sqlancer.common.schema.AbstractTables;

public class ClickHouseExpressionGenerator
        extends TypedExpressionGenerator<ClickHouseExpression, ClickHouseColumn, ClickHouseLancerDataType> implements
        NoRECGenerator<ClickHouseSelect, ClickHouseJoin, ClickHouseExpression, ClickHouseTable, ClickHouseColumn>,
        TLPWhereGenerator<ClickHouseSelect, ClickHouseJoin, ClickHouseExpression, ClickHouseTable, ClickHouseColumn> {

    private final ClickHouseGlobalState globalState;
    public boolean allowAggregateFunctions;

    private List<ClickHouseTable> tables;
    private final List<ClickHouseColumnReference> columnRefs;

    public ClickHouseExpressionGenerator(ClickHouseGlobalState globalState) {
        this.globalState = globalState;
        this.columnRefs = new ArrayList<>();
    }

    public final void addColumns(List<ClickHouseColumnReference> col) {
        this.columnRefs.addAll(col);
    }

    private enum ColumnLike {
        UNARY_PREFIX, BINARY_ARITHMETIC, UNARY_FUNCTION, BINARY_FUNCTION
    }

    private enum Expression {
        UNARY_PREFIX, BINARY_ARITHMETIC, UNARY_FUNCTION, BINARY_FUNCTION, BINARY_LOGICAL, BINARY_COMPARISON,
        UNARY_POSTFIX
    }

    public ClickHouseExpression generateExpressionWithColumns(List<ClickHouseColumnReference> columns, int remainingDepth) {
    if (columns.isEmpty() || remainingDepth <= 1 || Randomly.getBooleanWithRatherLowProbability()) {
        return generateConstant(null);
    }

    if (remainingDepth <= 2 || Randomly.getBooleanWithRatherLowProbability()) {
        return Randomly.fromList(columns);
    }

    ColumnLike expr = Randomly.fromOptions(ColumnLike.values());
    switch (expr) {
        case UNARY_PREFIX:
            return new ClickHouseUnaryPrefixOperation(
                generateExpressionWithColumns(columns, remainingDepth - 1),
                ClickHouseUnaryPrefixOperator.MINUS
            );
        case BINARY_ARITHMETIC:
            return new ClickHouseBinaryArithmeticOperation(
                generateExpressionWithColumns(columns, remainingDepth - 1),
                generateExpressionWithColumns(columns, remainingDepth - 1),
                ClickHouseBinaryArithmeticOperation.ClickHouseBinaryArithmeticOperator.getRandom()
            );
        case UNARY_FUNCTION:
            return new ClickHouseUnaryFunctionOperation(
                generateExpressionWithColumns(columns, remainingDepth - 1),
                ClickHouseUnaryFunctionOperation.ClickHouseUnaryFunctionOperator.getRandom()
            );
        case BINARY_FUNCTION:
            return new ClickHouseBinaryFunctionOperation(
                generateExpressionWithColumns(columns, remainingDepth - 1),
                generateExpressionWithColumns(columns, remainingDepth - 1),
                ClickHouseBinaryFunctionOperation.ClickHouseBinaryFunctionOperator.getRandom()
            );
        default:
            throw new AssertionError("Unexpected expression type: " + expr);
    }
}

 public ClickHouseExpression generateAggregateExpressionWithColumns(List<ClickHouseColumnReference> columns,
        int remainingDepth) {
    // Increase probability of using aggregation as depth decreases
    if (remainingDepth <= 2 || Randomly.getBoolean()) {
        return new ClickHouseAggregate(
            generateNumericExpressionWithColumns(columns, remainingDepth - 1),
            ClickHouseAggregate.ClickHouseAggregateFunction.getRandom()
        );
    }

    if (columns.isEmpty() || (remainingDepth <= 2 && Randomly.getBooleanWithRatherLowProbability())) {
        return Randomly.fromOptions(
            generateConstant(ClickHouseLancerDataType.getRandomNumeric()),
            new ClickHouseUnaryPrefixOperation(generateConstant(ClickHouseLancerDataType.getRandomNumeric()), ClickHouseUnaryPrefixOperator.MINUS),
            new ClickHouseBinaryArithmeticOperation(
                generateConstant(ClickHouseLancerDataType.getRandomNumeric()),
                generateConstant(ClickHouseLancerDataType.getRandomNumeric()),
                ClickHouseBinaryArithmeticOperation.ClickHouseBinaryArithmeticOperator.getRandom()
            )
        );
    }

    // Select only numeric columns
    List<ClickHouseColumnReference> numericColumns = columns.stream()
        .filter(c -> c.getColumn().getType().getType().isNumeric())
        .collect(Collectors.toList());

    if (!numericColumns.isEmpty()) {
        return Randomly.fromList(numericColumns);
    }

    ColumnLike expr = Randomly.fromOptions(ColumnLike.values());
    switch (expr) {
        case UNARY_PREFIX:
            return new ClickHouseUnaryPrefixOperation(
                generateExpressionWithColumns(columns, remainingDepth - 1),
                ClickHouseUnaryPrefixOperator.MINUS
            );
        case BINARY_ARITHMETIC:
            return new ClickHouseBinaryArithmeticOperation(
                generateExpressionWithColumns(columns, remainingDepth - 1),
                generateExpressionWithColumns(columns, remainingDepth - 1),
                ClickHouseBinaryArithmeticOperation.ClickHouseBinaryArithmeticOperator.getRandom()
            );
        case UNARY_FUNCTION:
            return new ClickHouseUnaryFunctionOperation(
                generateExpressionWithColumns(columns, remainingDepth - 1),
                ClickHouseUnaryFunctionOperation.ClickHouseUnaryFunctionOperator.getRandom()
            );
        case BINARY_FUNCTION:
            return new ClickHouseBinaryFunctionOperation(
                generateExpressionWithColumns(columns, remainingDepth - 1),
                generateExpressionWithColumns(columns, remainingDepth - 1),
                ClickHouseBinaryFunctionOperation.ClickHouseBinaryFunctionOperator.getRandom()
            );
        default:
            throw new AssertionError(expr);
    }
}

  public ClickHouseExpression generateExpressionWithExpression(List<ClickHouseExpression> expression,
        int remainingDepth) {
    if (remainingDepth <= 2 || Randomly.getBooleanWithRatherLowProbability()) {
        if (!expression.isEmpty() && Randomly.getBoolean()) {
            return Randomly.fromList(expression);
        } else {
            return Randomly.fromOptions(
                generateConstant(ClickHouseLancerDataType.UInt8),
                new ClickHouseUnaryPrefixOperation(generateConstant(ClickHouseLancerDataType.getRandomNumeric()),
                    ClickHouseUnaryPrefixOperator.MINUS),
                new ClickHouseBinaryArithmeticOperation(
                    generateConstant(ClickHouseLancerDataType.getRandomNumeric()),
                    generateConstant(ClickHouseLancerDataType.getRandomNumeric()),
                    ClickHouseBinaryArithmeticOperation.ClickHouseBinaryArithmeticOperator.getRandom()
                )
            );
        }
    }

    Expression type = Randomly.fromOptions(Expression.values());
    switch (type) {
        case UNARY_PREFIX:
            return new ClickHouseUnaryPrefixOperation(
                generateExpressionWithExpression(expression, remainingDepth - 1),
                ClickHouseUnaryPrefixOperation.ClickHouseUnaryPrefixOperator.getRandom()
            );
        case UNARY_POSTFIX:
            return new ClickHouseUnaryPostfixOperation(
                generateExpressionWithExpression(expression, remainingDepth - 1),
                ClickHouseUnaryPostfixOperation.ClickHouseUnaryPostfixOperator.getRandom(),
                false
            );
        case BINARY_COMPARISON:
            return new ClickHouseBinaryComparisonOperation(
                ensureBooleanExpression(generateExpressionWithExpression(expression, remainingDepth - 1)),
                ensureBooleanExpression(generateExpressionWithExpression(expression, remainingDepth - 1)),
                ClickHouseBinaryComparisonOperation.ClickHouseBinaryComparisonOperator.getRandomOperator()
            );
        case BINARY_LOGICAL:
            return new ClickHouseBinaryLogicalOperation(
                ensureBooleanExpression(generateExpressionWithExpression(expression, remainingDepth - 1)),
                ensureBooleanExpression(generateExpressionWithExpression(expression, remainingDepth - 1)),
                ClickHouseBinaryLogicalOperation.ClickHouseBinaryLogicalOperator.getRandom()
            );
        case BINARY_ARITHMETIC:
            return new ClickHouseBinaryArithmeticOperation(
                ensureNumericExpression(generateExpressionWithExpression(expression, remainingDepth - 1)),
                ensureNumericExpression(generateExpressionWithExpression(expression, remainingDepth - 1)),
                ClickHouseBinaryArithmeticOperation.ClickHouseBinaryArithmeticOperator.getRandom()
            );
        case UNARY_FUNCTION:
            return new ClickHouseUnaryFunctionOperation(
                generateExpressionWithExpression(expression, remainingDepth - 1),
                ClickHouseUnaryFunctionOperation.ClickHouseUnaryFunctionOperator.getRandom()
            );
        case BINARY_FUNCTION:
            return new ClickHouseBinaryFunctionOperation(
                generateExpressionWithExpression(expression, remainingDepth - 1),
                generateExpressionWithExpression(expression, remainingDepth - 1),
                ClickHouseBinaryFunctionOperation.ClickHouseBinaryFunctionOperator.getRandom()
            );
        default:
            throw new AssertionError(type);
    }
}

   @Override
protected ClickHouseExpression generateExpression(ClickHouseLancerDataType type, int depth) {
    if (allowAggregateFunctions && Randomly.getBooleanWithRatherLowProbability()) {
        ClickHouseLancerDataType aggType = ClickHouseLancerDataType.getRandomNumeric();
        return new ClickHouseAggregate(
                generateExpression(aggType, depth + 1),
                ClickHouseAggregate.ClickHouseAggregateFunction.getRandom());
    }

    if (depth >= globalState.getOptions().getMaxExpressionDepth() || Randomly.getBooleanWithRatherLowProbability()) {
        return generateLeafNode(type);
    }

    Expression expr = Randomly.fromOptions(Expression.values());
    ClickHouseLancerDataType leftLeafType = ClickHouseLancerDataType.getRandom();
    ClickHouseLancerDataType rightLeafType = Randomly.getBooleanWithRatherLowProbability() 
            ? leftLeafType 
            : ClickHouseLancerDataType.getRandom();

    switch (expr) {
        case UNARY_PREFIX:
            return new ClickHouseUnaryPrefixOperation(
                    generateExpression(leftLeafType, depth + 1),
                    ClickHouseUnaryPrefixOperation.ClickHouseUnaryPrefixOperator.getRandom());

        case UNARY_POSTFIX:
            return new ClickHouseUnaryPostfixOperation(
                    generateExpression(leftLeafType, depth + 1),
                    ClickHouseUnaryPostfixOperation.ClickHouseUnaryPostfixOperator.getRandom(), false);

        case BINARY_COMPARISON:
            return new ClickHouseBinaryComparisonOperation(
                    generateExpression(leftLeafType, depth + 1),
                    generateExpression(rightLeafType, depth + 1),
                    ClickHouseBinaryComparisonOperation.ClickHouseBinaryComparisonOperator.getRandomOperator());

        case BINARY_LOGICAL:
            return new ClickHouseBinaryLogicalOperation(
                    generateExpression(leftLeafType, depth + 1),
                    generateExpression(rightLeafType, depth + 1),
                    ClickHouseBinaryLogicalOperation.ClickHouseBinaryLogicalOperator.getRandom());

        case BINARY_ARITHMETIC:
            return new ClickHouseBinaryArithmeticOperation(
                    generateExpression(leftLeafType, depth + 1),
                    generateExpression(leftLeafType, depth + 1),
                    ClickHouseBinaryArithmeticOperation.ClickHouseBinaryArithmeticOperator.getRandom());

        case UNARY_FUNCTION:
            return new ClickHouseUnaryFunctionOperation(
                    generateExpression(leftLeafType, depth + 1),
                    ClickHouseUnaryFunctionOperation.ClickHouseUnaryFunctionOperator.getRandom());

        case BINARY_FUNCTION:
            return new ClickHouseBinaryFunctionOperation(
                    generateExpression(leftLeafType, depth + 1),
                    generateExpression(leftLeafType, depth + 1),
                    ClickHouseBinaryFunctionOperation.ClickHouseBinaryFunctionOperator.getRandom());

        default:
            throw new AssertionError("Unexpected Expression Type: " + expr);
    }
}

protected ClickHouseExpression.ClickHouseJoinOnClause generateJoinClause(
        ClickHouseTableReference leftTable, ClickHouseTableReference rightTable) {
    
    List<ClickHouseColumnReference> leftColumns = leftTable.getColumnReferences();
    List<ClickHouseColumnReference> rightColumns = rightTable.getColumnReferences();

    if (leftColumns.isEmpty() || rightColumns.isEmpty()) {
        throw new IllegalArgumentException("Join clause cannot be generated with empty column references.");
    }

    ClickHouseColumnReference leftColumn = Randomly.fromList(leftColumns);
    ClickHouseColumnReference rightColumn = Randomly.fromList(rightColumns);

    // Ensure both columns have the same data type for a valid JOIN condition
    if (leftColumn.getColumn().getType().getType() != rightColumn.getColumn().getType().getType()) {
        rightColumn = leftColumn;  // Fallback: Use the same column from both tables
    }

    ClickHouseExpression leftExpr = new ClickHouseColumnReference(leftColumn.getColumn(), leftTable);
    ClickHouseExpression rightExpr = new ClickHouseColumnReference(rightColumn.getColumn(), rightTable);

    return new ClickHouseExpression.ClickHouseJoinOnClause(leftExpr, rightExpr);
}

  @Override
protected ClickHouseExpression generateColumn(ClickHouseLancerDataType type) {
    if (columnRefs.isEmpty()) {
        return generateConstant(type);
    }

    List<ClickHouseColumnReference> filteredColumns = columnRefs.stream()
            .filter(c -> c.getColumn().getType().getType() == type.getType())
            .collect(Collectors.toList());

    return !filteredColumns.isEmpty() ? Randomly.fromList(filteredColumns) : Randomly.fromList(columnRefs);
}

protected ClickHouseExpression getColumnNameFromTable(ClickHouseSchema.ClickHouseTable table) {
    if (columnRefs.isEmpty()) {
        return generateConstant(ClickHouseLancerDataType.getRandom());
    }

    List<ClickHouseColumnReference> filteredColumns = columnRefs.stream()
            .filter(c -> c.getColumn().getTable().equals(table))
            .collect(Collectors.toList());

    return !filteredColumns.isEmpty() ? Randomly.fromList(filteredColumns) : generateConstant(ClickHouseLancerDataType.getRandom());
}

   @Override
protected ClickHouseLancerDataType getRandomType() {
    return ClickHouseLancerDataType.getRandom();
}

@Override
protected boolean canGenerateColumnOfType(ClickHouseLancerDataType type) {
    return true;
}

@Override
public ClickHouseExpression generateConstant(ClickHouseLancerDataType genType) {
    ClickHouseLancerDataType type = (genType == null) ? ClickHouseLancerDataType.getRandom() : genType;
    switch (type.getType()) {
        case Int8, UInt8, Int16, UInt16, Int32, UInt32, Int64, UInt64 -> 
            return ClickHouseCreateConstant.createIntConstant(type.getType(), globalState.getRandomly().getInteger());
        case Float32 -> 
            return ClickHouseCreateConstant.createFloat32Constant((float) globalState.getRandomly().getDouble());
        case Float64 -> 
            return ClickHouseCreateConstant.createFloat64Constant(globalState.getRandomly().getDouble());
        case String -> 
            return ClickHouseCreateConstant.createStringConstant(globalState.getRandomly().getString());
        default -> 
            throw new AssertionError("Unsupported type: " + type.getType());
    }
}

@Override
public ClickHouseExpression generateBooleanExpression() {
    List<ClickHouseColumnReference> columnRefs = columns.stream()
            .map(c -> c.asColumnReference(c.getTable().getName()))
            .filter(c -> c.getColumn().getType().getType() == ClickHouseDataType.UInt8)
            .collect(Collectors.toList());

    if (columnRefs.isEmpty()) {
        return new ClickHouseUnaryFunctionOperation(
                generateExpression(ClickHouseLancerDataType.UInt8, 3),
                ClickHouseUnaryFunctionOperation.ClickHouseUnaryFunctionOperator.NOT);
    }
    
    return generateExpressionWithColumns(columnRefs, 5);
}

@Override
public ClickHouseSelect generateSelect() {
    return new ClickHouseSelect();
}

@Override
public List<ClickHouseExpression.ClickHouseJoin> getRandomJoinClauses(ClickHouseTableReference left, 
        List<ClickHouseSchema.ClickHouseTable> tables) {
    List<ClickHouseExpression.ClickHouseJoin> joinStatements = new ArrayList<>();
    
    if (!globalState.getDbmsSpecificOptions().testJoins || tables.isEmpty()) {
        return joinStatements;
    }

    List<ClickHouseTableReference> leftTables = new ArrayList<>();
    leftTables.add(left);
    
    int nrJoinClauses = Randomly.getNotCachedInteger(0, tables.size());
    for (int i = 0; i < nrJoinClauses; i++) {
        ClickHouseTableReference leftTable = Randomly.fromList(leftTables);
        ClickHouseTableReference rightTable = new ClickHouseTableReference(Randomly.fromList(tables), "right_" + i);
        
        ClickHouseExpression.ClickHouseJoinOnClause joinClause = generateJoinClause(leftTable, rightTable);
        ClickHouseExpression.ClickHouseJoin.JoinType options = Randomly.fromOptions(ClickHouseExpression.ClickHouseJoin.JoinType.values());
        
        ClickHouseExpression.ClickHouseJoin join = new ClickHouseExpression.ClickHouseJoin(leftTable, rightTable, options, joinClause);
        joinStatements.add(join);
        leftTables.add(rightTable);
    }
    
    return joinStatements;
}

@Override
public List<ClickHouseExpression> getTableRefs() {
    return tables.stream()
            .map(t -> new ClickHouseTableReference(t, null))
            .collect(Collectors.toList());
}

@Override
public String generateOptimizedQueryString(ClickHouseSelect select, ClickHouseExpression whereCondition, 
        boolean shouldUseAggregate) {
    List<ClickHouseColumn> filteredColumns = Randomly.extractNrRandomColumns(columns, 
            Randomly.getNotCachedInteger(1, columns.size()));

    if (shouldUseAggregate) {
        ClickHouseAggregate aggr = new ClickHouseAggregate(
                new ClickHouseColumnReference(ClickHouseColumn.createDummy("*", null), null, null),
                ClickHouseAggregateFunction.COUNT);
        select.setFetchColumns(List.of(aggr));
    } else {
        select.setFetchColumns(filteredColumns.stream()
                .map(c -> c.asColumnReference(c.getTable().getName()))
                .collect(Collectors.toList()));
    }
    
    select.setWhereClause(whereCondition);
    return select.asString();
}

@Override
public String generateUnoptimizedQueryString(ClickHouseSelect select, ClickHouseExpression whereCondition) {
    ClickHouseExpression inner = new ClickHouseAliasOperation(whereCondition, "check");
    
    select.setFetchColumns(List.of(inner));
    select.setWhereClause(null);
    
    return "SELECT SUM(`check` <> 0) FROM (" + select.asString() + ") AS result_alias";
}

@Override
public List<ClickHouseExpression> generateFetchColumns(boolean shouldCreateDummy) {
    if (shouldCreateDummy) {
        return List.of(new ClickHouseColumnReference(ClickHouseColumn.createDummy("*", null), null, null));
    }
    
    List<ClickHouseColumnReference> columnReferences = columns.stream()
            .map(c -> c.asColumnReference(c.getTable().getName()))
            .collect(Collectors.toList());

    return IntStream.range(0, 1 + Randomly.smallNumber())
            .mapToObj(i -> generateExpressionWithColumns(columnReferences, 5))
            .collect(Collectors.toList());
}
