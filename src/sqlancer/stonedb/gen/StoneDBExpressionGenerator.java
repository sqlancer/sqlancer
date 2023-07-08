package sqlancer.stonedb.gen;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.common.ast.newast.ColumnReferenceNode;
import sqlancer.common.ast.newast.NewBetweenOperatorNode;
import sqlancer.common.ast.newast.NewBinaryOperatorNode;
import sqlancer.common.ast.newast.NewCaseOperatorNode;
import sqlancer.common.ast.newast.NewFunctionNode;
import sqlancer.common.ast.newast.NewInOperatorNode;
import sqlancer.common.ast.newast.NewUnaryPostfixOperatorNode;
import sqlancer.common.ast.newast.NewUnaryPrefixOperatorNode;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.gen.UntypedExpressionGenerator;
import sqlancer.stonedb.StoneDBProvider.StoneDBGlobalState;
import sqlancer.stonedb.StoneDBSchema.StoneDBColumn;
import sqlancer.stonedb.StoneDBSchema.StoneDBDataType;
import sqlancer.stonedb.StoneDBToStringVisitor;
import sqlancer.stonedb.ast.StoneDBConstant;
import sqlancer.stonedb.ast.StoneDBExpression;

public class StoneDBExpressionGenerator extends UntypedExpressionGenerator<Node<StoneDBExpression>, StoneDBColumn> {

    private final StoneDBGlobalState globalState;

    public StoneDBExpressionGenerator(StoneDBGlobalState globalState) {
        this.globalState = globalState;
    }

    private enum Expression {
        UNARY_PREFIX, UNARY_POSTFIX, BINARY_COMPARISON, BINARY_LOGICAL, BINARY_ARITHMETIC, BINARY_BITWISE, BETWEEN, IN,
        CASE
    }

    public static class StoneDBCastOperation extends NewUnaryPostfixOperatorNode<StoneDBExpression> {
        public StoneDBCastOperation(Node<StoneDBExpression> expr, StoneDBDataType type) {
            super(expr, () -> "CAST(" + StoneDBToStringVisitor.asString(expr) + " AS "
                    + (type == StoneDBDataType.INT ? "SIGNED" : type.name()) + ")");
        }
    }

    @Override
    public Node<StoneDBExpression> negatePredicate(Node<StoneDBExpression> predicate) {
        return new NewUnaryPrefixOperatorNode<>(predicate, StoneDBUnaryPrefixOperator.NOT);
    }

    @Override
    public Node<StoneDBExpression> isNull(Node<StoneDBExpression> expr) {
        return new NewUnaryPostfixOperatorNode<>(expr, StoneDBUnaryPostfixOperator.IS_NULL);
    }

    @Override
    public Node<StoneDBExpression> generateConstant() {
        if (Randomly.getBooleanWithSmallProbability()) {
            return StoneDBConstant.createNullConstant();
        }
        StoneDBDataType type = StoneDBDataType.getRandomWithoutNull();
        return generateConstant(type);
    }

    public Node<StoneDBExpression> generateConstant(StoneDBDataType dataType) {
        switch (dataType) {
        case INT:
            return StoneDBConstant
                    .createIntConstant(globalState.getRandomly().getInteger(Integer.MIN_VALUE + 1, Integer.MAX_VALUE));
        case DATE:
            return StoneDBConstant.createDateConstant(globalState.getRandomly().getInteger());
        case TIMESTAMP:
            return StoneDBConstant.createTimestampConstant(globalState.getRandomly().getInteger());
        case VARCHAR:
            return StoneDBConstant.createTextConstant(globalState.getRandomly().getString());
        case DOUBLE:
            return StoneDBConstant.createDoubleConstant(globalState.getRandomly().getDouble());
        default:
            throw new IgnoreMeException();
        }
    }

    public Node<StoneDBExpression> generateConstant(StoneDBDataType dataType, boolean isNullable) {
        if (isNullable && Randomly.getBooleanWithSmallProbability()) {
            generateConstant(StoneDBDataType.NULL);
        }
        return generateConstant(dataType);
    }

    @Override
    protected Node<StoneDBExpression> generateExpression(int depth) {
        if (depth >= globalState.getOptions().getMaxExpressionDepth() || Randomly.getBoolean()) {
            return generateLeafNode();
        }
        if (allowAggregates && Randomly.getBoolean()) {
            StoneDBAggregateFunction aggregateFunction = StoneDBAggregateFunction.getRandom();
            allowAggregates = false;
            return new NewFunctionNode<>(generateExpressions(aggregateFunction.getNrArgs(), depth + 1),
                    aggregateFunction);
        }
        List<Expression> possibleOptions = new ArrayList<>(Arrays.asList(Expression.values()));
        Expression expr = Randomly.fromList(possibleOptions);
        Operator op;
        switch (expr) {
        case UNARY_PREFIX:
            op = StoneDBUnaryPrefixOperator.getRandom();
            return new NewUnaryPrefixOperatorNode<>(generateExpression(depth + 1), op);
        case UNARY_POSTFIX:
            op = StoneDBUnaryPostfixOperator.getRandom();
            return new NewUnaryPostfixOperatorNode<>(generateExpression(depth + 1), op);
        case BINARY_COMPARISON:
            op = StoneDBBinaryComparisonOperator.getRandom();
            return new NewBinaryOperatorNode<>(generateExpression(depth + 1), generateExpression(depth + 1), op);
        case BINARY_LOGICAL:
            op = StoneDBBinaryLogicalOperator.getRandom();
            return new NewBinaryOperatorNode<>(generateExpression(depth + 1), generateExpression(depth + 1), op);
        case BINARY_ARITHMETIC:
            op = StoneDBBinaryArithmeticOperator.getRandom();
            return new NewBinaryOperatorNode<>(generateExpression(depth + 1), generateExpression(depth + 1), op);
        case BINARY_BITWISE:
            op = StoneDBBinaryBitwiseOperator.getRandom();
            return new NewBinaryOperatorNode<>(generateExpression(depth + 1), generateExpression(depth + 1), op);
        case BETWEEN:
            return new NewBetweenOperatorNode<>(generateExpression(depth + 1), generateExpression(depth + 1),
                    generateExpression(depth + 1), Randomly.getBoolean());
        case IN:
            return new NewInOperatorNode<>(generateExpression(depth + 1),
                    generateExpressions(Randomly.smallNumber() + 1, depth + 1), Randomly.getBoolean());
        case CASE:
            int nr = Randomly.smallNumber() + 1;
            return new NewCaseOperatorNode<>(generateExpression(depth + 1), generateExpressions(nr, depth + 1),
                    generateExpressions(nr, depth + 1), generateExpression(depth + 1));
        default:
            throw new AssertionError();
        }
    }

    @Override
    protected Node<StoneDBExpression> generateColumn() {
        StoneDBColumn column = Randomly.fromList(columns);
        return new ColumnReferenceNode<>(column);
    }

    public enum StoneDBAggregateFunction {
        MAX(1), MIN(1), AVG(1), COUNT(1), FIRST(1), SUM(1);

        private int nrArgs;

        StoneDBAggregateFunction(int nrArgs) {
            this.nrArgs = nrArgs;
        }

        public static StoneDBAggregateFunction getRandom() {
            return Randomly.fromOptions(values());
        }

        public int getNrArgs() {
            return nrArgs;
        }
    }

    public enum StoneDBUnaryPrefixOperator implements Operator {

        NOT("NOT"), PLUS("+"), MINUS("-");

        private final String textRepr;

        StoneDBUnaryPrefixOperator(String textRepr) {
            this.textRepr = textRepr;
        }

        @Override
        public String getTextRepresentation() {
            return textRepr;
        }

        public static StoneDBUnaryPrefixOperator getRandom() {
            return Randomly.fromOptions(values());
        }
    }

    public enum StoneDBUnaryPostfixOperator implements Operator {

        IS_NULL("IS NULL"), IS_NOT_NULL("IS NOT NULL");

        private final String textRepr;

        StoneDBUnaryPostfixOperator(String textRepr) {
            this.textRepr = textRepr;
        }

        @Override
        public String getTextRepresentation() {
            return textRepr;
        }

        public static StoneDBUnaryPostfixOperator getRandom() {
            return Randomly.fromOptions(values());
        }
    }

    /*
     * Comparison operators supported by StoneDB: https://stonedb.io/docs/SQL-reference/operators/comparison-operators
     */
    public enum StoneDBBinaryComparisonOperator implements Operator {
        EQUAL("="), GREATER(">"), LESS("<"), GREATER_EQUAL(">="), LESS_EQUAL("<="),
        NOT_EQUALS(Randomly.fromList(Arrays.asList("!=", "<>"))), NULL_SAFE_EQUAL("<=>"), IN("IN"), NOT_IN("NOT_IN"),
        LIKE("LIKE"), IS_NULL("IS NULL"), IS_NOT_NULL("IS NOT NULL");

        private final String textRepr;

        StoneDBBinaryComparisonOperator(String textRepr) {
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

    /*
     * Logical operators supported by StoneDB: https://stonedb.io/docs/SQL-reference/operators/logical-operators
     */
    public enum StoneDBBinaryLogicalOperator implements Operator {

        NOT("NOT"), AND("AND"), OR("OR"), XOR("XOR");

        private final String textRepr;

        StoneDBBinaryLogicalOperator(String textRepr) {
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

    /*
     * Arithmetic operators supported by StoneDB: https://stonedb.io/docs/SQL-reference/operators/arithmetic-operators
     */
    public enum StoneDBBinaryArithmeticOperator implements Operator {
        ADDITION("+"), MINUS("-"), MULTIPLICATION("*"), DIVISION(Randomly.fromList(Arrays.asList("/", " div "))),
        MODULO(Randomly.fromList(Arrays.asList("%", "  mod ")));

        private final String textRepr;

        StoneDBBinaryArithmeticOperator(String textRepr) {
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

    /*
     * Bitwise operators supported by StoneDB: https://stonedb.io/docs/SQL-reference/operators/bitwise-operators
     */
    public enum StoneDBBinaryBitwiseOperator implements Operator {
        AND("&"), OR("|"), XOR("^"), INVERSION("!"), LEFTSHIFT("<<"), RIGHTSHIFT(">>");

        private final String textRepr;

        StoneDBBinaryBitwiseOperator(String textRepr) {
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
}
