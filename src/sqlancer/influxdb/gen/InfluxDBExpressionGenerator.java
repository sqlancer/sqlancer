package sqlancer.influxdb.gen;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.gen.UntypedExpressionGenerator;
import sqlancer.common.schema.AbstractTables;
import sqlancer.influxdb.InfluxDBProvider.InfluxDBGlobalState;
import sqlancer.influxdb.InfluxDBSchema.InfluxDBColumn;
import sqlancer.influxdb.InfluxDBSchema.InfluxDBCompositeDataType;
import sqlancer.influxdb.InfluxDBSchema.InfluxDBDataType;
import sqlancer.influxdb.InfluxDBSchema.InfluxDBTable;
import sqlancer.influxdb.ast.*;

public final class InfluxDBExpressionGenerator extends UntypedExpressionGenerator<InfluxDBExpression, InfluxDBColumn> {

    private final InfluxDBGlobalState globalState;
    private List<InfluxDBTable> tables;

    public InfluxDBExpressionGenerator(InfluxDBGlobalState globalState) {
        this.globalState = globalState;
    }

    private enum Expression {
        BINARY_COMPARISON, BINARY_LOGICAL, BINARY_ARITHMETIC, AGGREGATE_FUNC, FIELD_SELECTION, TIMESTAMP, TAG
    }

    @Override
    protected InfluxDBExpression generateExpression(int depth) {
        if (depth >= globalState.getOptions().getMaxExpressionDepth() || Randomly.getBoolean()) {
            return generateLeafNode();
        }
        List<Expression> possibleOptions = new ArrayList<>(Arrays.asList(Expression.values()));
        Expression expr = Randomly.fromList(possibleOptions);
        switch (expr) {
            case BINARY_COMPARISON:
                InfluxDBBinaryComparisonOperator comparisonOp = InfluxDBBinaryComparisonOperator.getRandom();
                return new InfluxDBBinaryOperation(generateExpression(depth + 1), generateExpression(depth + 1), comparisonOp);
            case BINARY_LOGICAL:
                InfluxDBBinaryLogicalOperator logicalOp = InfluxDBBinaryLogicalOperator.getRandom();
                return new InfluxDBBinaryOperation(generateExpression(depth + 1), generateExpression(depth + 1), logicalOp);
            case BINARY_ARITHMETIC:
                InfluxDBBinaryArithmeticOperator arithmeticOp = InfluxDBBinaryArithmeticOperator.getRandom();
                return new InfluxDBBinaryOperation(generateExpression(depth + 1), generateExpression(depth + 1), arithmeticOp);
            case AGGREGATE_FUNC:
                InfluxDBAggregateFunction aggregateFunc = InfluxDBAggregateFunction.getRandom();
                return new InfluxDBFunction<>(generateExpressions(aggregateFunc.getNrArgs(), depth + 1), aggregateFunc);
            case FIELD_SELECTION:
                return new InfluxDBFieldReference(generateColumn());
            case TIMESTAMP:
                return InfluxDBConstant.createTimestampConstant(globalState.getRandomly().getInteger());
            case TAG:
                return new InfluxDBTagReference(generateColumn().getName(), globalState.getRandomly().getString());
            default:
                throw new AssertionError();
        }
    }

    @Override
    protected InfluxDBExpression generateColumn() {
        InfluxDBColumn column = Randomly.fromList(columns);
        return new InfluxDBColumnReference(column);
    }

    @Override
    public InfluxDBExpression generateConstant() {
        if (Randomly.getBooleanWithSmallProbability()) {
            return InfluxDBConstant.createNullConstant();
        }
        InfluxDBDataType type = InfluxDBDataType.getRandomWithoutNull();
        switch (type) {
            case INT:
                return InfluxDBConstant.createIntConstant(globalState.getRandomly().getInteger());
            case BOOLEAN:
                return InfluxDBConstant.createBooleanConstant(Randomly.getBoolean());
            case FLOAT:
                return InfluxDBConstant.createFloatConstant(globalState.getRandomly().getDouble());
            case STRING:
                return InfluxDBConstant.createStringConstant(globalState.getRandomly().getString());
            case TIMESTAMP:
                return InfluxDBConstant.createTimestampConstant(globalState.getRandomly().getInteger());
            default:
                throw new AssertionError();
        }
    }

    @Override
    public List<InfluxDBExpression> generateOrderBys() {
        List<InfluxDBExpression> expr = super.generateOrderBys();
        List<InfluxDBExpression> newExpr = new ArrayList<>(expr.size());
        for (InfluxDBExpression curExpr : expr) {
            if (Randomly.getBoolean()) {
                curExpr = new InfluxDBOrderingTerm(curExpr, InfluxDBOrdering.getRandom());
            }
            newExpr.add(curExpr);
        }
        return newExpr;
    };

    public enum InfluxDBBinaryLogicalOperator implements Operator {
        AND, OR;

        @Override
        public String getTextRepresentation() {
            return toString();
        }

        public static InfluxDBBinaryLogicalOperator getRandom() {
            return Randomly.fromOptions(values());
        }
    }

    public enum InfluxDBBinaryArithmeticOperator implements Operator {
        ADD("+"), SUB("-"), MULT("*"), DIV("/");

        private String textRepr;

        InfluxDBBinaryArithmeticOperator(String textRepr) {
            this.textRepr = textRepr;
        }

        @Override
        public String getTextRepresentation() {
            return textRepr;
        }

        public static InfluxDBBinaryArithmeticOperator getRandom() {
            return Randomly.fromOptions(values());
        }
    }

    public enum InfluxDBBinaryComparisonOperator implements Operator {
        EQUALS("="), GREATER_THAN(">"), GREATER_THAN_EQUALS(">="), LESS_THAN("<"), LESS_THAN_EQUALS("<="), NOT_EQUALS("!=");

        private String textRepr;

        InfluxDBBinaryComparisonOperator(String textRepr) {
            this.textRepr = textRepr;
        }

        @Override
        public String getTextRepresentation() {
            return textRepr;
        }

        public static InfluxDBBinaryComparisonOperator getRandom() {
            return Randomly.fromOptions(values());
        }
    }

    public enum InfluxDBAggregateFunction {
        MEAN(1), COUNT(1), SUM(1), MIN(1), MAX(1);

        private int nrArgs;

        InfluxDBAggregateFunction(int nrArgs) {
            this.nrArgs = nrArgs;
        }

        public static InfluxDBAggregateFunction getRandom() {
            return Randomly.fromOptions(values());
        }

        public int getNrArgs() {
            return nrArgs;
        }
    }

    @Override
    public InfluxDBExpression negatePredicate(InfluxDBExpression predicate) {
        return new InfluxDBUnaryPrefixOperator(predicate, InfluxDBUnaryPrefixOperator.Operator.NOT);
    }

    @Override
    public InfluxDBExpression isNull(InfluxDBExpression expr) {
        return new InfluxDBUnaryPostfixOperator(expr, InfluxDBUnaryPostfixOperator.Operator.IS_NULL);
    }

    @Override
    public InfluxDBExpressionGenerator setTablesAndColumns(AbstractTables<InfluxDBTable, InfluxDBColumn> tables) {
        this.columns = tables.getColumns();
        this.tables = tables.getTables();
        return this;
    }

    public InfluxDBExpression generateBooleanExpression() {
        return generateExpression();
    }

    @Override
    public InfluxDBSelect generateSelect() {
        return new InfluxDBSelect();
    }

    public List<InfluxDBJoin> getRandomJoinClauses() {
        List<InfluxDBTableReference> tableList = tables.stream().map(InfluxDBTableReference::new).collect(Collectors.toList());
        List<InfluxDBJoin> joins = InfluxDBJoin.getJoins(tableList, globalState);
        tables = tableList.stream().map(InfluxDBTableReference::getTable).collect(Collectors.toList());
        return joins;
    }

    public List<InfluxDBExpression> getTableRefs() {
        return tables.stream().map(InfluxDBTableReference::new).collect(Collectors.toList());
    }

    public String generateOptimizedQueryString(InfluxDBSelect select, InfluxDBExpression whereCondition) {
        List<InfluxDBExpression> allColumns = columns.stream().map(InfluxDBColumnReference::new).collect(Collectors.toList());
        select.setFetchColumns(allColumns);
        if (Randomly.getBooleanWithSmallProbability()) {
            select.setOrderByClauses(generateOrderBys());
        }
        select.setWhereClause(whereCondition);
        return select.asString();
    }

    public List<InfluxDBExpression> generateFetchColumns() {
        if (Randomly.getBoolean()) {
            return List.of(new InfluxDBColumnReference(new InfluxDBColumn("*", InfluxDBDataType.STRING, false, false)));
        }
        return Randomly.nonEmptySubset(columns).stream().map(InfluxDBColumnReference::new).collect(Collectors.toList());
    }
}