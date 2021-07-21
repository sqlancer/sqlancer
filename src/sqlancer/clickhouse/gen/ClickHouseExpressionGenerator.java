package sqlancer.clickhouse.gen;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import ru.yandex.clickhouse.domain.ClickHouseDataType;
import sqlancer.Randomly;
import sqlancer.clickhouse.ClickHouseProvider.ClickHouseGlobalState;
import sqlancer.clickhouse.ClickHouseSchema;
import sqlancer.clickhouse.ClickHouseSchema.ClickHouseColumn;
import sqlancer.clickhouse.ClickHouseSchema.ClickHouseLancerDataType;
import sqlancer.clickhouse.ast.ClickHouseAggregate;
import sqlancer.clickhouse.ast.ClickHouseBinaryComparisonOperation;
import sqlancer.clickhouse.ast.ClickHouseBinaryLogicalOperation;
import sqlancer.clickhouse.ast.ClickHouseColumnReference;
import sqlancer.clickhouse.ast.ClickHouseConstant;
import sqlancer.clickhouse.ast.ClickHouseExpression;
import sqlancer.clickhouse.ast.ClickHouseUnaryPostfixOperation;
import sqlancer.clickhouse.ast.ClickHouseUnaryPostfixOperation.ClickHouseUnaryPostfixOperator;
import sqlancer.clickhouse.ast.ClickHouseUnaryPrefixOperation;
import sqlancer.clickhouse.ast.ClickHouseUnaryPrefixOperation.ClickHouseUnaryPrefixOperator;
import sqlancer.common.gen.TypedExpressionGenerator;

public class ClickHouseExpressionGenerator
        extends TypedExpressionGenerator<ClickHouseExpression, ClickHouseColumn, ClickHouseLancerDataType> {

    private final ClickHouseGlobalState globalState;
    public boolean allowAggregateFunctions;

    public ClickHouseExpressionGenerator(ClickHouseGlobalState globalState) {
        this.globalState = globalState;
    }

    private enum Expression {
        UNARY_POSTFIX, UNARY_PREFIX, BINARY_COMPARISON, BINARY_LOGICAL
    }

    @Override
    protected ClickHouseExpression generateExpression(ClickHouseLancerDataType type, int depth) {
        if (allowAggregateFunctions && Randomly.getBoolean()) {
            return generateAggregate();
        }
        if (depth >= globalState.getOptions().getMaxExpressionDepth() || Randomly.getBoolean()) {
            return generateLeafNode(type);
        }
        Expression expr = Randomly.fromOptions(Expression.values());
        ClickHouseLancerDataType leftLeafType = ClickHouseLancerDataType.getRandom();
        ClickHouseLancerDataType rightLeafType = ClickHouseLancerDataType.getRandom();
        if (Randomly.getBoolean()) {
            rightLeafType = leftLeafType;
        }

        switch (expr) {
        case UNARY_PREFIX:
            return new ClickHouseUnaryPrefixOperation(generateExpression(leftLeafType, depth + 1),
                    ClickHouseUnaryPrefixOperation.ClickHouseUnaryPrefixOperator.getRandom());
        case UNARY_POSTFIX:
            return new ClickHouseUnaryPostfixOperation(generateExpression(leftLeafType, depth + 1),
                    ClickHouseUnaryPostfixOperation.ClickHouseUnaryPostfixOperator.getRandom(), false);
        case BINARY_COMPARISON:
            return new ClickHouseBinaryComparisonOperation(generateExpression(leftLeafType, depth + 1),
                    generateExpression(rightLeafType, depth + 1),
                    ClickHouseBinaryComparisonOperation.ClickHouseBinaryComparisonOperator.getRandomOperator());
        case BINARY_LOGICAL:
            return new ClickHouseBinaryLogicalOperation(generateExpression(leftLeafType, depth + 1),
                    generateExpression(rightLeafType, depth + 1),
                    ClickHouseBinaryLogicalOperation.ClickHouseBinaryLogicalOperator.getRandom());
        default:
            throw new AssertionError(expr);
        }
    }

    @Override
    protected ClickHouseExpression generateColumn(ClickHouseLancerDataType type) {
        if (columns.isEmpty()) {
            return generateConstant(type);
        }
        List<ClickHouseColumn> filteredColumns = columns.stream()
                .filter(c -> c.getType().getType().name().equals(type.getType().name())).collect(Collectors.toList());
        ClickHouseColumn column = filteredColumns.isEmpty() ? Randomly.fromList(columns)
                : Randomly.fromList(filteredColumns);
        return new ClickHouseColumnReference(column, null);
    }

    @Override
    protected ClickHouseLancerDataType getRandomType() {
        return ClickHouseLancerDataType.getRandom();
    }

    public List<ClickHouseExpression.ClickHouseJoin> getRandomJoinClauses(
            List<ClickHouseSchema.ClickHouseTable> tables) {
        List<ClickHouseExpression.ClickHouseJoin> joinStatements = new ArrayList<>();
        if (!globalState.getDbmsSpecificOptions().testJoins) {
            return joinStatements;
        }
        if (Randomly.getBoolean() && tables.size() > 1) {
            int nrJoinClauses = (int) Randomly.getNotCachedInteger(0, tables.size());
            for (int i = 0; i < nrJoinClauses; i++) {
                ClickHouseExpression joinClause = generateExpression(ClickHouseLancerDataType.getRandom());
                ClickHouseSchema.ClickHouseTable table = Randomly.fromList(tables);
                tables.remove(table);
                ClickHouseExpression.ClickHouseJoin.JoinType options;
                options = Randomly.fromOptions(ClickHouseExpression.ClickHouseJoin.JoinType.values());
                if (options == ClickHouseExpression.ClickHouseJoin.JoinType.NATURAL) {
                    // NATURAL joins do not have an ON clause
                    joinClause = null;
                }
                ClickHouseExpression.ClickHouseJoin j = new ClickHouseExpression.ClickHouseJoin(table, joinClause,
                        options);
                joinStatements.add(j);
            }

        }
        return joinStatements;
    }

    @Override
    protected boolean canGenerateColumnOfType(ClickHouseLancerDataType type) {
        return true;
    }

    @Override
    public ClickHouseExpression generateConstant(ClickHouseLancerDataType type) {
        switch (type.getType()) {
        case Int8:
        case UInt8:
        case Int16:
        case UInt16:
        case Int32:
        case UInt32:
        case Int64:
        case UInt64:
            return ClickHouseConstant.createIntConstant(type.getType(), globalState.getRandomly().getInteger());
        case Float32:
            return ClickHouseConstant.createFloat32Constant((float) globalState.getRandomly().getDouble());
        case Float64:
            return ClickHouseConstant.createFloat64Constant(globalState.getRandomly().getDouble());
        case String:
            return ClickHouseConstant.createStringConstant(globalState.getRandomly().getString());
        default:
            throw new AssertionError();
        }
    }

    public ClickHouseExpression getHavingClause() {
        allowAggregateFunctions = true;
        return generateExpression(new ClickHouseLancerDataType(ClickHouseDataType.UInt8));
    }

    public ClickHouseAggregate generateArgsForAggregate(ClickHouseDataType dataType,
            ClickHouseAggregate.ClickHouseAggregateFunction agg) {
        List<ClickHouseDataType> types = agg.getTypes(dataType);
        List<ClickHouseExpression> args = new ArrayList<>();
        for (ClickHouseDataType argType : types) {
            this.allowAggregateFunctions = false;
            args.add(generateExpression(new ClickHouseLancerDataType(argType)));
            this.allowAggregateFunctions = true;
        }
        return new ClickHouseAggregate(args, agg);
    }

    public ClickHouseExpressionGenerator allowAggregates(boolean value) {
        allowAggregateFunctions = value;
        return this;
    }

    public ClickHouseExpression generateAggregate() {
        return getAggregate(ClickHouseLancerDataType.getRandom().getType());
    }

    private ClickHouseExpression getAggregate(ClickHouseDataType dataType) {
        List<ClickHouseAggregate.ClickHouseAggregateFunction> aggregates = ClickHouseAggregate.ClickHouseAggregateFunction
                .getAggregates(dataType);
        ClickHouseAggregate.ClickHouseAggregateFunction agg = Randomly.fromList(aggregates);
        return generateArgsForAggregate(dataType, agg);
    }

    @Override
    public ClickHouseExpression generatePredicate() {
        return generateExpression(new ClickHouseSchema.ClickHouseLancerDataType(ClickHouseDataType.UInt8));
    }

    @Override
    public ClickHouseExpression negatePredicate(ClickHouseExpression predicate) {
        return new ClickHouseUnaryPrefixOperation(predicate, ClickHouseUnaryPrefixOperator.NOT);
    }

    @Override
    public ClickHouseExpression isNull(ClickHouseExpression expr) {
        return new ClickHouseUnaryPostfixOperation(expr, ClickHouseUnaryPostfixOperator.IS_NULL, false);
    }
}
