package sqlancer.yugabyte.ycql.gen;

import static sqlancer.yugabyte.YugabyteBugs.bug14330;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.common.ast.newast.ColumnReferenceNode;
import sqlancer.common.ast.newast.NewBetweenOperatorNode;
import sqlancer.common.ast.newast.NewBinaryOperatorNode;
import sqlancer.common.ast.newast.NewFunctionNode;
import sqlancer.common.ast.newast.NewInOperatorNode;
import sqlancer.common.ast.newast.NewOrderingTerm;
import sqlancer.common.ast.newast.NewOrderingTerm.Ordering;
import sqlancer.common.ast.newast.NewUnaryPostfixOperatorNode;
import sqlancer.common.ast.newast.NewUnaryPrefixOperatorNode;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.gen.UntypedExpressionGenerator;
import sqlancer.yugabyte.ycql.YCQLProvider.YCQLGlobalState;
import sqlancer.yugabyte.ycql.YCQLSchema.YCQLColumn;
import sqlancer.yugabyte.ycql.YCQLSchema.YCQLDataType;
import sqlancer.yugabyte.ycql.ast.YCQLConstant;
import sqlancer.yugabyte.ycql.ast.YCQLExpression;

public final class YCQLExpressionGenerator extends UntypedExpressionGenerator<Node<YCQLExpression>, YCQLColumn> {

    private final YCQLGlobalState globalState;

    public YCQLExpressionGenerator(YCQLGlobalState globalState) {
        this.globalState = globalState;
    }

    private enum Expression {
        BINARY_COMPARISON, BINARY_LOGICAL, BINARY_ARITHMETIC, FUNC, BETWEEN, IN
    }

    @Override
    protected Node<YCQLExpression> generateExpression(int depth) {
        if (depth >= globalState.getOptions().getMaxExpressionDepth() || Randomly.getBoolean()) {
            return generateLeafNode();
        }
        if (allowAggregates && Randomly.getBoolean()) {
            YCQLAggregateFunction aggregate = YCQLAggregateFunction.getRandom();
            allowAggregates = false;
            return new NewFunctionNode<>(generateExpressions(depth + 1, aggregate.getNrArgs()), aggregate);
        }
        List<Expression> possibleOptions = new ArrayList<>(Arrays.asList(Expression.values()));
        Expression expr = Randomly.fromList(possibleOptions);
        switch (expr) {
        case BINARY_COMPARISON:
            Operator op = YCQLBinaryComparisonOperator.getRandom();
            return new NewBinaryOperatorNode<YCQLExpression>(generateExpression(depth + 1),
                    generateExpression(depth + 1), op);
        case BINARY_LOGICAL:
            op = YCQLBinaryLogicalOperator.getRandom();
            return new NewBinaryOperatorNode<YCQLExpression>(generateExpression(depth + 1),
                    generateExpression(depth + 1), op);
        case BINARY_ARITHMETIC:
            return new NewBinaryOperatorNode<YCQLExpression>(generateExpression(depth + 1),
                    generateExpression(depth + 1), YCQLBinaryArithmeticOperator.getRandom());
        case FUNC:
            DBFunction func = DBFunction.getRandom();
            return new NewFunctionNode<YCQLExpression, DBFunction>(generateExpressions(func.getNrArgs()), func);
        case BETWEEN:
            return new NewBetweenOperatorNode<YCQLExpression>(generateExpression(depth + 1),
                    generateExpression(depth + 1), generateExpression(depth + 1), Randomly.getBoolean());
        case IN:
            return new NewInOperatorNode<YCQLExpression>(generateExpression(depth + 1),
                    generateExpressions(depth + 1, Randomly.smallNumber() + 1), Randomly.getBoolean());
        default:
            throw new AssertionError(expr);
        }
    }

    @Override
    protected Node<YCQLExpression> generateColumn() {
        YCQLColumn column = Randomly.fromList(columns);
        return new ColumnReferenceNode<YCQLExpression, YCQLColumn>(column);
    }

    @Override
    public Node<YCQLExpression> generateConstant() {
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
    public List<Node<YCQLExpression>> generateOrderBys() {
        List<Node<YCQLExpression>> expr = super.generateOrderBys();
        List<Node<YCQLExpression>> newExpr = new ArrayList<>(expr.size());
        for (Node<YCQLExpression> curExpr : expr) {
            if (Randomly.getBoolean()) {
                curExpr = new NewOrderingTerm<>(curExpr, Ordering.getRandom());
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

    public NewFunctionNode<YCQLExpression, YCQLAggregateFunction> generateArgsForAggregate(
            YCQLAggregateFunction aggregateFunction) {
        return new NewFunctionNode<YCQLExpression, YCQLAggregateFunction>(
                generateExpressions(aggregateFunction.getNrArgs()), aggregateFunction);
    }

    public Node<YCQLExpression> generateAggregate() {
        YCQLAggregateFunction aggrFunc = YCQLAggregateFunction.getRandom();
        return generateArgsForAggregate(aggrFunc);
    }

    @Override
    public Node<YCQLExpression> negatePredicate(Node<YCQLExpression> predicate) {
        return new NewUnaryPrefixOperatorNode<>(predicate, YCQLUnaryPrefixOperator.NOT);
    }

    @Override
    public Node<YCQLExpression> isNull(Node<YCQLExpression> expr) {
        return new NewUnaryPostfixOperatorNode<>(expr, YCQLUnaryPostfixOperator.IS_NULL);
    }

}
