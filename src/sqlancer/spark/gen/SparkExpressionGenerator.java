package sqlancer.spark.gen;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.common.ast.newast.NewOrderingTerm.Ordering;
import sqlancer.common.gen.TLPWhereGenerator;
import sqlancer.common.gen.UntypedExpressionGenerator;
import sqlancer.common.schema.AbstractTables;
import sqlancer.spark.SparkGlobalState;
import sqlancer.spark.SparkSchema.SparkColumn;
import sqlancer.spark.SparkSchema.SparkDataType;
import sqlancer.spark.SparkSchema.SparkTable;
import sqlancer.spark.ast.SparkBetweenOperation;
import sqlancer.spark.ast.SparkBinaryOperation;
import sqlancer.spark.ast.SparkCaseOperation;
import sqlancer.spark.ast.SparkCastOperation;
import sqlancer.spark.ast.SparkColumnReference;
import sqlancer.spark.ast.SparkConstant;
import sqlancer.spark.ast.SparkExpression;
import sqlancer.spark.ast.SparkFunction;
import sqlancer.spark.ast.SparkInOperation;
import sqlancer.spark.ast.SparkJoin;
import sqlancer.spark.ast.SparkOrderingTerm;
import sqlancer.spark.ast.SparkSelect;
import sqlancer.spark.ast.SparkTableReference;
import sqlancer.spark.ast.SparkUnaryPostfixOperation;
import sqlancer.spark.ast.SparkUnaryPrefixOperation;

public class SparkExpressionGenerator extends UntypedExpressionGenerator<SparkExpression, SparkColumn>
        implements TLPWhereGenerator<SparkSelect, SparkJoin, SparkExpression, SparkTable, SparkColumn> {

    private final SparkGlobalState globalState;
    private List<SparkTable> tables;

    private enum Expression {
        UNARY_PREFIX, UNARY_POSTFIX, BINARY_COMPARISON, BINARY_LOGICAL, BINARY_ARITHMETIC, CAST, FUNC, BETWEEN, IN,
        CASE;
    }

    public SparkExpressionGenerator(SparkGlobalState globalState) {
        this.globalState = globalState;
    }

    @Override
    public SparkExpression negatePredicate(SparkExpression predicate) {
        return new SparkUnaryPrefixOperation(predicate, SparkUnaryPrefixOperator.NOT);
    }

    @Override
    public SparkExpression isNull(SparkExpression expr) {
        return new SparkUnaryPostfixOperation(expr, SparkUnaryPostfixOperator.IS_NULL);
    }

    @Override
    protected SparkExpression generateExpression(int depth) {
        return generateExpressionInternal(depth);
    }

    private SparkExpression generateExpressionInternal(int depth) throws AssertionError {
        if (depth >= globalState.getOptions().getMaxExpressionDepth()
                || Randomly.getBooleanWithRatherLowProbability()) {
            return generateLeafNode();
        }
        if (allowAggregates && Randomly.getBooleanWithRatherLowProbability()) {
            allowAggregates = false; // aggregate function calls cannot be nested
            SparkAggregateFunction aggregate = SparkAggregateFunction.getRandom();
            return new SparkFunction<>(generateExpressions(aggregate.getNrArgs(), depth + 1), aggregate);
        }

        List<Expression> possibleOptions = new ArrayList<>(Arrays.asList(Expression.values()));
        Expression expr = Randomly.fromList(possibleOptions);

        switch (expr) {
        case UNARY_PREFIX:
            return new SparkUnaryPrefixOperation(generateExpression(depth + 1), SparkUnaryPrefixOperator.getRandom());
        case UNARY_POSTFIX:
            return new SparkUnaryPostfixOperation(generateExpression(depth + 1), SparkUnaryPostfixOperator.getRandom());
        case BINARY_COMPARISON:
            Operator op = SparkBinaryComparisonOperator.getRandom();
            return new SparkBinaryOperation(generateExpression(depth + 1), generateExpression(depth + 1), op);
        case BINARY_LOGICAL:
            op = SparkBinaryLogicalOperator.getRandom();
            return new SparkBinaryOperation(generateExpression(depth + 1), generateExpression(depth + 1), op);
        case BINARY_ARITHMETIC:
            return new SparkBinaryOperation(generateExpression(depth + 1), generateExpression(depth + 1),
                    SparkBinaryArithmeticOperator.getRandom());
        case CAST:
            return new SparkCastOperation(generateExpression(depth + 1), SparkDataType.getRandomType());
        case FUNC:
            SparkFunc func = SparkFunc.getRandom();
            return new SparkFunction<>(generateExpressions(func.getNrArgs()), func);
        case BETWEEN:
            return new SparkBetweenOperation(generateExpression(depth + 1), generateExpression(depth + 1),
                    generateExpression(depth + 1), Randomly.getBoolean());
        case IN:
            return new SparkInOperation(generateExpression(depth + 1),
                    generateExpressions(Randomly.smallNumber() + 1, depth + 1), Randomly.getBoolean());
        case CASE:
            int nr = Randomly.smallNumber() + 1;
            return new SparkCaseOperation(generateExpression(depth + 1), generateExpressions(nr, depth + 1),
                    generateExpressions(nr, depth + 1), generateExpression(depth + 1));
        default:
            throw new AssertionError(expr);
        }
    }

    @Override
    public SparkExpression generateConstant() {
        if (Randomly.getBooleanWithRatherLowProbability()) {
            return SparkConstant.createNullConstant();
        }
        SparkDataType[] values = SparkDataType.values();
        SparkDataType constantType = Randomly.fromOptions(values);
        switch (constantType) {
        case STRING:
            return SparkConstant.createStringConstant(globalState.getRandomly().getString());
        case INTEGER:
            return SparkConstant.createIntConstant(globalState.getRandomly().getInteger());
        case DOUBLE:
            return SparkConstant.createDoubleConstant(globalState.getRandomly().getDouble());
        case BOOLEAN:
            return SparkConstant.createBooleanConstant(Randomly.getBoolean());
        case TIMESTAMP:
            return SparkConstant.createTimestampConstant(globalState.getRandomly().getInteger());
        case DATE:
            return SparkConstant.createDateConstant(globalState.getRandomly().getInteger());
        default:
            throw new AssertionError(constantType);
        }
    }

    @Override
    protected SparkExpression generateColumn() {
        SparkColumn column = Randomly.fromList(columns);
        return new SparkColumnReference(column);
    }

    @Override
    public List<SparkExpression> generateOrderBys() {
        List<SparkExpression> expr = super.generateOrderBys();
        List<SparkExpression> newExpr = new ArrayList<>(expr.size());
        for (SparkExpression curExpr : expr) {
            if (Randomly.getBoolean()) {
                curExpr = new SparkOrderingTerm(curExpr, Ordering.getRandom());
            }
            newExpr.add(curExpr);
        }
        return newExpr;
    }

    @Override
    public SparkExpressionGenerator setTablesAndColumns(AbstractTables<SparkTable, SparkColumn> tables) {
        this.columns = tables.getColumns();
        this.tables = tables.getTables();
        return this;
    }

    @Override
    public SparkExpression generateBooleanExpression() {
        return generateExpression();
    }

    @Override
    public SparkSelect generateSelect() {
        return new SparkSelect();
    }

    @Override
    public List<SparkExpression> getTableRefs() {
        return tables.stream().map(t -> new SparkTableReference(t)).collect(Collectors.toList());
    }

    @Override
    public List<SparkExpression> generateFetchColumns(boolean allowAggregates) {
        if (Randomly.getBoolean()) {
            return List.of(new SparkColumnReference(new SparkColumn("*", null, null)));
        }
        return Randomly.nonEmptySubset(columns).stream().map(c -> new SparkColumnReference(c))
                .collect(Collectors.toList());
    }

    @Override
    public List<SparkJoin> getRandomJoinClauses() {
        return List.of();
    }

    public enum SparkUnaryPrefixOperator implements Operator {
        NOT("NOT"), PLUS("+"), MINUS("-"), BITWISE_NOT("~");

        private String textRepr;

        SparkUnaryPrefixOperator(String textRepr) {
            this.textRepr = textRepr;
        }

        public static SparkUnaryPrefixOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return textRepr;
        }
    }

    public enum SparkUnaryPostfixOperator implements Operator {
        IS_NULL("IS NULL"), IS_NOT_NULL("IS NOT NULL");

        private String textRepr;

        SparkUnaryPostfixOperator(String textRepr) {
            this.textRepr = textRepr;
        }

        public static SparkUnaryPostfixOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return textRepr;
        }
    }

    public enum SparkBinaryComparisonOperator implements Operator {
        EQUALS("="), GREATER(">"), GREATER_EQUALS(">="), SMALLER("<"), SMALLER_EQUALS("<="), NOT_EQUALS("!="),
        LIKE("LIKE"), NOT_LIKE("NOT LIKE"), RLIKE("RLIKE");

        private String textRepr;

        SparkBinaryComparisonOperator(String textRepr) {
            this.textRepr = textRepr;
        }

        public static SparkBinaryComparisonOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return textRepr;
        }
    }

    public enum SparkBinaryLogicalOperator implements Operator {
        AND("AND"), OR("OR");

        private String textRepr;

        SparkBinaryLogicalOperator(String textRepr) {
            this.textRepr = textRepr;
        }

        public static SparkBinaryLogicalOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return textRepr;
        }
    }

    public enum SparkBinaryArithmeticOperator implements Operator {
        // Spark supports || for concat, and bitwise operators &, |, ^
        CONCAT("||"), ADD("+"), SUB("-"), MULT("*"), DIV("/"), MOD("%"), BITWISE_AND("&"), BITWISE_OR("|"),
        BITWISE_XOR("^");

        private String textRepr;

        SparkBinaryArithmeticOperator(String textRepr) {
            this.textRepr = textRepr;
        }

        public static SparkBinaryArithmeticOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return textRepr;
        }
    }

    public enum SparkAggregateFunction {
        COUNT(1), SUM(1), AVG(1), MIN(1), MAX(1), VARIANCE(1), VAR_SAMP(1), STDDEV_POP(1), STDDEV_SAMP(1), COVAR_POP(2),
        COVAR_SAMP(2), CORR(2);

        private int nrArgs;

        SparkAggregateFunction(int nrArgs) {
            this.nrArgs = nrArgs;
        }

        public static SparkAggregateFunction getRandom() {
            return Randomly.fromOptions(values());
        }

        public int getNrArgs() {
            return nrArgs;
        }
    }

    public enum SparkFunc {
        ROUND(2), FLOOR(1), ABS(1), CEIL(1);

        private int nrArgs;
        private boolean isVariadic;

        SparkFunc(int nrArgs) {
            this(nrArgs, false);
        }

        SparkFunc(int nrArgs, boolean isVariadic) {
            this.nrArgs = nrArgs;
            this.isVariadic = isVariadic;
        }

        public static SparkFunc getRandom() {
            return Randomly.fromOptions(values());
        }

        public int getNrArgs() {
            if (isVariadic) {
                return Randomly.smallNumber() + nrArgs;
            } else {
                return nrArgs;
            }
        }
    }
}