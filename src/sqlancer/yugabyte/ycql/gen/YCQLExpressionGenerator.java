package sqlancer.yugabyte.ycql.gen;

import static sqlancer.yugabyte.YugabyteBugs.bug14330;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.common.ast.newast.NewOrderingTerm.Ordering;
import sqlancer.common.gen.UntypedExpressionGenerator;
import sqlancer.yugabyte.ycql.YCQLProvider.YCQLGlobalState;
import sqlancer.yugabyte.ycql.YCQLSchema.YCQLColumn;
import sqlancer.yugabyte.ycql.YCQLSchema.YCQLDataType;
import sqlancer.yugabyte.ycql.ast.YCQLBetweenOperation;
import sqlancer.yugabyte.ycql.ast.YCQLBinaryOperation;
import sqlancer.yugabyte.ycql.ast.YCQLColumnReference;
import sqlancer.yugabyte.ycql.ast.YCQLConstant;
import sqlancer.yugabyte.ycql.ast.YCQLExpression;
import sqlancer.yugabyte.ycql.ast.YCQLFunction;
import sqlancer.yugabyte.ycql.ast.YCQLInOperation;
import sqlancer.yugabyte.ycql.ast.YCQLOrderingTerm;
import sqlancer.yugabyte.ycql.ast.YCQLUnaryPostfixOperation;
import sqlancer.yugabyte.ycql.ast.YCQLUnaryPrefixOperation;

public final class YCQLExpressionGenerator extends UntypedExpressionGenerator<YCQLExpression, YCQLColumn> {

    private final YCQLGlobalState globalState;

    public YCQLExpressionGenerator(YCQLGlobalState globalState) {
        this.globalState = globalState;
    }

    private enum Expression {
        BINARY_COMPARISON, BINARY_LOGICAL, BINARY_ARITHMETIC, FUNC, BETWEEN, IN
    }

    @Override
    protected YCQLExpression generateExpression(int depth) {
        if (depth >= globalState.getOptions().getMaxExpressionDepth() || Randomly.getBoolean()) {
            return generateLeafNode();
        }
        if (allowAggregates && Randomly.getBoolean()) {
            YCQLAggregateFunction aggregate = YCQLAggregateFunction.getRandom();
            allowAggregates = false;
            return new YCQLFunction<>(generateExpressions(depth + 1, aggregate.getNrArgs()), aggregate);
        }
        List<Expression> possibleOptions = new ArrayList<>(Arrays.asList(Expression.values()));
        Expression expr = Randomly.fromList(possibleOptions);
        switch (expr) {
        case BINARY_COMPARISON:
            Operator op = YCQLBinaryComparisonOperator.getRandom();
            return new YCQLBinaryOperation(generateExpression(depth + 1), generateExpression(depth + 1), op);
        case BINARY_LOGICAL:
            op = YCQLBinaryLogicalOperator.getRandom();
            return new YCQLBinaryOperation(generateExpression(depth + 1), generateExpression(depth + 1), op);
        case BINARY_ARITHMETIC:
            return new YCQLBinaryOperation(generateExpression(depth + 1), generateExpression(depth + 1),
                    YCQLBinaryArithmeticOperator.getRandom());
        case FUNC:
            DBFunction func = DBFunction.getRandom();
            return new YCQLFunction<DBFunction>(generateExpressions(func.getNrArgs()), func);
        case BETWEEN:
            return new YCQLBetweenOperation(generateExpression(depth + 1), generateExpression(depth + 1),
                    generateExpression(depth + 1), Randomly.getBoolean());
        case IN:
            return new YCQLInOperation(generateExpression(depth + 1),
                    generateExpressions(depth + 1, Randomly.smallNumber() + 1), Randomly.getBoolean());
        default:
            throw new AssertionError(expr);
        }
    }

    @Override
    protected YCQLExpression generateColumn() {
        YCQLColumn column = Randomly.fromList(columns);
        return new YCQLColumnReference(column);
    }

    @Override
    public YCQLExpression generateConstant() {
        if (Randomly.getBooleanWithSmallProbability()) {
            if (bug14330) {
                throw new IgnoreMeException();
            }

            return YCQLConstant.createNullConstant();
        }
        YCQLDataType type = YCQLDataType.getRandom();
        switch (type) {
        case INT:
            return YCQLConstant.createIntConstant(globalState.getRandomly().getInteger());
        case DATE:
            return YCQLConstant.createDateConstant(globalState.getRandomly().getInteger());
        case TIMESTAMP:
            return YCQLConstant.createTimestampConstant(globalState.getRandomly().getInteger());
        case VARCHAR:
            return YCQLConstant.createStringConstant(globalState.getRandomly().getString());
        case BOOLEAN:
            return YCQLConstant.createBooleanConstant(Randomly.getBoolean());
        case FLOAT:
            return YCQLConstant.createFloatConstant(globalState.getRandomly().getDouble());
        default:
            throw new AssertionError();
        }
    }

    @Override
    public List<YCQLExpression> generateOrderBys() {
        List<YCQLExpression> expr = super.generateOrderBys();
        List<YCQLExpression> newExpr = new ArrayList<>(expr.size());
        for (YCQLExpression curExpr : expr) {
            if (Randomly.getBoolean()) {
                curExpr = new YCQLOrderingTerm(curExpr, Ordering.getRandom());
            }
            newExpr.add(curExpr);
        }
        return newExpr;
    };

    public enum YCQLAggregateFunction {
        MAX(1), MIN(1), AVG(1), COUNT(1), SUM(1);

        private final int nrArgs;

        YCQLAggregateFunction(int nrArgs) {
            this.nrArgs = nrArgs;
        }

        public static YCQLAggregateFunction getRandom() {
            return Randomly.fromOptions(values());
        }

        public int getNrArgs() {
            return nrArgs;
        }

    }

    public enum DBFunction {
        // YCQL functions
        BLOB(1), //
        TIMEUUID(1), //
        DATE(0), //
        TIME(0), //
        TIMESTAMP(0), //
        BIGINT(1), //
        UUID(0); //
        // // extras
        // PARTITION_HASH(2), //
        // WRITETIME(1), //
        // TTL(1); //

        private final int nrArgs;
        private final boolean isVariadic;

        DBFunction(int nrArgs) {
            this(nrArgs, false);
        }

        DBFunction(int nrArgs, boolean isVariadic) {
            this.nrArgs = nrArgs;
            this.isVariadic = isVariadic;
        }

        public static DBFunction getRandom() {
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

    public enum YCQLUnaryPostfixOperator implements Operator {

        IS_NULL("IS NULL"), IS_NOT_NULL("IS NOT NULL");

        private final String textRepr;

        YCQLUnaryPostfixOperator(String textRepr) {
            this.textRepr = textRepr;
        }

        @Override
        public String getTextRepresentation() {
            return textRepr;
        }

        public static YCQLUnaryPostfixOperator getRandom() {
            return Randomly.fromOptions(values());
        }

    }

    public enum YCQLUnaryPrefixOperator implements Operator {

        NOT("NOT"), PLUS("+"), MINUS("-");

        private final String textRepr;

        YCQLUnaryPrefixOperator(String textRepr) {
            this.textRepr = textRepr;
        }

        @Override
        public String getTextRepresentation() {
            return textRepr;
        }

        public static YCQLUnaryPrefixOperator getRandom() {
            return Randomly.fromOptions(values());
        }

    }

    public enum YCQLBinaryLogicalOperator implements Operator {

        AND, OR;

        @Override
        public String getTextRepresentation() {
            return toString();
        }

        public static Operator getRandom() {
            return Randomly.fromOptions(values());
        }

    }

    public enum YCQLBinaryArithmeticOperator implements Operator {
        ADD("+"), SUB("-"), MULT("*"), DIV("/");

        private String textRepr;

        YCQLBinaryArithmeticOperator(String textRepr) {
            this.textRepr = textRepr;
        }

        public static Operator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return textRepr;
        }

    }

    public enum YCQLBinaryComparisonOperator implements Operator {

        EQUALS("="), GREATER(">"), GREATER_EQUALS(">="), SMALLER("<"), SMALLER_EQUALS("<="), NOT_EQUALS("!=");

        private final String textRepr;

        YCQLBinaryComparisonOperator(String textRepr) {
            this.textRepr = textRepr;
        }

        public static Operator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return textRepr;
        }

    }

    public YCQLFunction<YCQLAggregateFunction> generateArgsForAggregate(YCQLAggregateFunction aggregateFunction) {
        return new YCQLFunction<YCQLAggregateFunction>(generateExpressions(aggregateFunction.getNrArgs()),
                aggregateFunction);
    }

    public YCQLExpression generateAggregate() {
        YCQLAggregateFunction aggrFunc = YCQLAggregateFunction.getRandom();
        return generateArgsForAggregate(aggrFunc);
    }

    @Override
    public YCQLExpression negatePredicate(YCQLExpression predicate) {
        return new YCQLUnaryPrefixOperation(predicate, YCQLUnaryPrefixOperator.NOT);
    }

    @Override
    public YCQLExpression isNull(YCQLExpression expr) {
        return new YCQLUnaryPostfixOperation(expr, YCQLUnaryPostfixOperator.IS_NULL);
    }

}
