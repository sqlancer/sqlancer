package sqlancer.hive.gen;

import sqlancer.Randomly;
import sqlancer.common.ast.newast.NewOrderingTerm.Ordering;
import sqlancer.common.gen.UntypedExpressionGenerator;
import sqlancer.hive.HiveGlobalState;
import sqlancer.hive.HiveSchema.*;
import sqlancer.hive.ast.HiveBetweenOperation;
import sqlancer.hive.ast.HiveBinaryOperation;
import sqlancer.hive.ast.HiveCaseOperation;
import sqlancer.hive.ast.HiveCastOperation;
import sqlancer.hive.ast.HiveColumnReference;
import sqlancer.hive.ast.HiveConstant;
import sqlancer.hive.ast.HiveExpression;
import sqlancer.hive.ast.HiveFunction;
import sqlancer.hive.ast.HiveInOperation;
import sqlancer.hive.ast.HiveOrderingTerm;
import sqlancer.hive.ast.HiveUnaryPrefixOperation;
import sqlancer.hive.ast.HiveUnaryPostfixOperation;
import sqlancer.common.ast.BinaryOperatorNode.Operator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class HiveExpressionGenerator extends UntypedExpressionGenerator<HiveExpression, HiveColumn> {

    private final HiveGlobalState globalState;

    private enum Expression {
        // TODO: add or delete expressions.
        UNARY_PREFIX, UNARY_POSTFIX, BINARY_COMPARISON, BINARY_LOGICAL, BINARY_ARITHMETIC,
        CAST, FUNC, BETWEEN, IN, CASE;
    }

    private enum ConstantType {
        // TODO: add DECIMAL, DATE, TIMESTAMP, BINARY,...
        STRING, INT, DOUBLE, BOOLEAN, NULL
    }

    public HiveExpressionGenerator(HiveGlobalState globalState) {
        this.globalState = globalState;
    }

    @Override
    public HiveExpression negatePredicate(HiveExpression predicate) {
        return new HiveUnaryPrefixOperation(predicate, HiveUnaryPrefixOperator.NOT);
    }

    @Override
    public HiveExpression isNull(HiveExpression expr) {
        return new HiveUnaryPostfixOperation(expr, HiveUnaryPostfixOperator.IS_NULL);
    }

    @Override
    protected HiveExpression generateExpression(int depth) {
        // TODO: randomly cast some types like what PostgresExpressionGenerator does?
        return generateExpressionInternal(depth);
    }

    private HiveExpression generateExpressionInternal(int depth) throws AssertionError {
            if (depth >= globalState.getOptions().getMaxExpressionDepth() 
                || Randomly.getBooleanWithRatherLowProbability()) {
            return generateLeafNode();
        }
        if (allowAggregates && Randomly.getBooleanWithRatherLowProbability()) {
            allowAggregates= false; // aggregate function calls cannot be nested
            HiveAggregateFunction aggregate = HiveAggregateFunction.getRandom();
            return new HiveFunction<>(generateExpressions(aggregate.getNrArgs(), depth + 1), aggregate);
        }

        List<Expression> possibleOptions = new ArrayList<>(Arrays.asList(Expression.values()));
        // TODO: remove some of the possible expression types according to options.

        Expression expr = Randomly.fromList(possibleOptions);
        switch (expr) {
            case UNARY_PREFIX:
                return new HiveUnaryPrefixOperation(generateExpression(depth + 1),
                        HiveUnaryPrefixOperator.getRandom());
            case UNARY_POSTFIX:
                return new HiveUnaryPostfixOperation(generateExpression(depth + 1),
                        HiveUnaryPostfixOperator.getRandom());
            case BINARY_COMPARISON:
                Operator op = HiveBinaryComparisonOperator.getRandom();
                return new HiveBinaryOperation(generateExpression(depth + 1),
                        generateExpression(depth + 1), op);
            case BINARY_LOGICAL:
                op = HiveExpressionGenerator.HiveBinaryLogicalOperator.getRandom();
                return new HiveBinaryOperation(generateExpression(depth + 1),
                        generateExpression(depth + 1), op);
            case BINARY_ARITHMETIC:
                return new HiveBinaryOperation(generateExpression(depth + 1),
                        generateExpression(depth + 1),
                        HiveExpressionGenerator.HiveBinaryArithmeticOperator.getRandom());
            case CAST:
                // return new HiveCastOperation(generateExpression(depth + 1),
                        // HiveSchema.HiveCompositeDataType.getRandomWithoutNull());
                return new HiveCastOperation(generateExpression(depth + 1), HiveDataType.getRandomType());
            case FUNC:
                HiveFunc func = HiveFunc.getRandom();
                return new HiveFunction<>(generateExpressions(func.getNrArgs()), func);
            case BETWEEN:
                return new HiveBetweenOperation(generateExpression(depth + 1),
                        generateExpression(depth + 1),
                        generateExpression(depth + 1),
                        Randomly.getBoolean());
            case IN:
                return new HiveInOperation(generateExpression(depth + 1),
                        generateExpressions(Randomly.smallNumber() + 1, depth + 1),
                        Randomly.getBoolean());
            case CASE:
                int nr = Randomly.smallNumber() + 1;
                return new HiveCaseOperation(generateExpression(depth + 1),
                        generateExpressions(nr, depth + 1),
                        generateExpressions(nr, depth + 1),
                        generateExpression(depth + 1));
            default:
                throw new AssertionError(expr);
        }
    }

    @Override
    public HiveExpression generateConstant() {
        ConstantType[] values = ConstantType.values();
        ConstantType constantType = Randomly.fromOptions(values);
        switch (constantType) {
            case STRING:
                return HiveConstant.createStringConstant(globalState.getRandomly().getString());
            case INT:
                return HiveConstant.createIntConstant(globalState.getRandomly().getInteger());
            case DOUBLE:
                return HiveConstant.createDoubleConstant(globalState.getRandomly().getDouble());
            case BOOLEAN:
                return HiveConstant.createBooleanConstant(Randomly.getBoolean());
            case NULL:
                return HiveConstant.createNullConstant();
            default:
                throw new AssertionError(constantType);
        }
    }

    @Override
    protected HiveExpression generateColumn() {
        HiveColumn column = Randomly.fromList(columns);
        return new HiveColumnReference(column);
    }

    @Override
    public List<HiveExpression> generateOrderBys() {
        List<HiveExpression> expr = super.generateOrderBys();
        List<HiveExpression> newExpr = new ArrayList<>(expr.size());
        for (HiveExpression curExpr : expr) {
            if (Randomly.getBoolean()) {
                curExpr = new HiveOrderingTerm(curExpr, Ordering.getRandom());
            }
            newExpr.add(curExpr);
        }
        return newExpr;
    }

    public enum HiveUnaryPrefixOperator implements Operator {

        // TODO: ~A (bitwise NOT)
        NOT("NOT"), PLUS("+"), MINUS("-");

        private String textRepr;

        HiveUnaryPrefixOperator(String textRepr) {
            this.textRepr = textRepr;
        }

        public static HiveUnaryPrefixOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return textRepr;
        }
    }

    public enum HiveUnaryPostfixOperator implements Operator {

        // TODO: A IS [NOT] (NULL|TRUE|FALSE)...
        IS_NULL("IS NULL"), IS_NOT_NULL("IS NOT NULL");

        private String textRepr;

        HiveUnaryPostfixOperator(String textRepr) {
            this.textRepr = textRepr;
        }

        public static HiveUnaryPostfixOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return textRepr;
        }
    }

    public enum HiveBinaryComparisonOperator implements Operator {

        EQUALS("="), GREATER(">"), GREATER_EQUALS(">="), SMALLER("<"),
        SMALLER_EQUALS("<="), NOT_EQUALS("!="), LIKE("LIKE"),
        NOT_LIKE("NOT LIKE"), REGEXP("RLIKE");

        private String textRepr;

        HiveBinaryComparisonOperator(String textRepr) {
            this.textRepr = textRepr;
        }

        public static HiveBinaryComparisonOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return textRepr;
        }
    }

    public enum HiveBinaryLogicalOperator implements Operator {

        AND("AND"), OR("OR");

        private String textRepr;

        HiveBinaryLogicalOperator(String textRepr) {
            this.textRepr = textRepr;
        }

        public static HiveBinaryLogicalOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return textRepr;
        }
    }

    public enum HiveBinaryArithmeticOperator implements Operator {

        CONCAT("||"), ADD("+"), SUB("-"), MULT("*"), DIV("/"), MOD("%"), BITWISE_AND("&"), BITWISE_OR("|"),
        BITWISE_XOR("^");

        private String textRepr;

        HiveBinaryArithmeticOperator(String textRepr) {
            this.textRepr = textRepr;
        }

        public static HiveBinaryArithmeticOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return textRepr;
        }
    }

    public enum HiveAggregateFunction {
        COUNT(1),
        SUM(1),
        AVG(1),
        MIN(1),
        MAX(1),
        VARIANCE(1),
        VAR_SAMP(1),
        STDDEV_POP(1),
        STDDEV_SAMP(1),
        COVAR_POP(2),
        COVAR_SAMP(2),
        CORR(2);

        private int nrArgs;

        HiveAggregateFunction(int nrArgs) {
            this.nrArgs = nrArgs;
        }

        public static HiveAggregateFunction getRandom() {
            return Randomly.fromOptions(values());
        }

        public int getNrArgs() {
            return nrArgs;
        }
    }

    // TODO: test all Hive default functions...
    public enum HiveFunc {
      
        // mathematical functions
        ROUND(2),
        FLOOR(1);

        // collection functions

        // date functions

        // string functions
        

        private int nrArgs;
        private boolean isVariadic;

        HiveFunc(int nrArgs) {
            this(nrArgs, false);
        }

        HiveFunc(int nrArgs, boolean isVariadic) {
            this.nrArgs = nrArgs;
            this.isVariadic = isVariadic;
        }

        public static HiveFunc getRandom() {
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
