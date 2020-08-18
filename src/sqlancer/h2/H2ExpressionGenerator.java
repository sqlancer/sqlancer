package sqlancer.h2;

import java.util.Random;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.common.ast.newast.ColumnReferenceNode;
import sqlancer.common.ast.newast.NewBetweenOperatorNode;
import sqlancer.common.ast.newast.NewBinaryOperatorNode;
import sqlancer.common.ast.newast.NewCaseOperatorNode;
import sqlancer.common.ast.newast.NewInOperatorNode;
import sqlancer.common.ast.newast.NewUnaryPostfixOperatorNode;
import sqlancer.common.ast.newast.NewUnaryPrefixOperatorNode;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.gen.UntypedExpressionGenerator;
import sqlancer.h2.H2Provider.H2GlobalState;
import sqlancer.h2.H2Schema.H2Column;
import sqlancer.h2.H2Schema.H2CompositeDataType;
import sqlancer.h2.H2Schema.H2DataType;

public class H2ExpressionGenerator extends UntypedExpressionGenerator<Node<H2Expression>, H2Column> {

    private static final Random RANDOM = new Random();

    private final H2GlobalState globalState;

    public H2ExpressionGenerator(H2GlobalState globalState) {
        this.globalState = globalState;
    }

    private enum Expression {
        BINARY_COMPARISON, BINARY_LOGICAL, UNARY_POSTFIX, UNARY_PREFIX, IN, BETWEEN, CASE, BINARY_ARITHMETIC, CAST;
    }

    @Override
    protected Node<H2Expression> generateExpression(int depth) {
        if (depth >= globalState.getOptions().getMaxExpressionDepth() || Randomly.getBoolean()) {
            return generateLeafNode();
        }
        Expression expr = Randomly.fromOptions(Expression.values());
        switch (expr) {
        case BINARY_COMPARISON:
            Operator op = H2BinaryComparisonOperator.getRandom();
            return new NewBinaryOperatorNode<H2Expression>(generateExpression(depth + 1), generateExpression(depth + 1),
                    op);
        case BINARY_LOGICAL:
            op = H2BinaryLogicalOperator.getRandom();
            return new NewBinaryOperatorNode<H2Expression>(generateExpression(depth + 1), generateExpression(depth + 1),
                    op);
        case UNARY_POSTFIX:
            op = H2UnaryPostfixOperator.getRandom();
            return new NewUnaryPostfixOperatorNode<H2Expression>(generateExpression(depth + 1), op);
        case UNARY_PREFIX:
            return new NewUnaryPrefixOperatorNode<H2Expression>(generateExpression(depth + 1),
                    H2UnaryPrefixOperator.getRandom());
        case IN:
            return new NewInOperatorNode<H2Expression>(generateExpression(depth + 1),
                    generateExpressions(depth + 1, Randomly.smallNumber() + 1), Randomly.getBoolean());
        case BETWEEN:
            return new NewBetweenOperatorNode<H2Expression>(generateExpression(depth + 1),
                    generateExpression(depth + 1), generateExpression(depth + 1), Randomly.getBoolean());
        case CASE:
            int nr = Randomly.smallNumber() + 1;
            return new NewCaseOperatorNode<H2Expression>(generateExpression(depth + 1),
                    generateExpressions(depth + 1, nr), generateExpressions(depth + 1, nr),
                    generateExpression(depth + 1));
        case BINARY_ARITHMETIC:
            return new NewBinaryOperatorNode<H2Expression>(generateExpression(depth + 1), generateExpression(depth + 1),
                    H2BinaryArithmeticOperator.getRandom());
        case CAST:
            return new H2CastNode(generateExpression(depth + 1), H2CompositeDataType.getRandom());
        default:
            throw new AssertionError();
        }
    }

    @Override
    protected Node<H2Expression> generateColumn() {
        return new ColumnReferenceNode<H2Expression, H2Column>(Randomly.fromList(columns));
    }

    @Override
    public Node<H2Expression> generateConstant() {
        if (Randomly.getBooleanWithSmallProbability()) {
            return H2Constant.createNullConstant();
        }
        switch (H2DataType.getRandom()) {
        case INT:
            return H2Constant.createIntConstant(getUncachedInt());
        case BOOL:
            return H2Constant.createBoolConstant(Randomly.getBoolean());
        case VARCHAR:
            return H2Constant.createStringConstant(Character.toString((char) (RANDOM.nextInt('z' - 'a') + 'a')));
        case DOUBLE:
            return H2Constant.createDoubleConstant(getUncachedDouble());
        case BINARY:
            return H2Constant.createBinaryConstant(getUncachedInt());
        default:
            throw new AssertionError();
        }
    }

    public static int getUncachedInt() {
        return RANDOM.nextInt();
    }

    public static double getUncachedDouble() {
        return RANDOM.nextDouble();
    }

    public enum H2UnaryPostfixOperator implements Operator {

        IS_NULL("IS NULL"), IS_NOT_NULL("IS NOT NULL"), IS_TRUE("IS TRUE"), IS_NOT_TRUE("IS NOT TRUE"),
        IS_FALSE("IS FALSE"), IS_NOT_FALSE("IS NOT FALSE"), IS_UNKNOWN("IS NOT UNKNOWN");

        private String textRepr;

        H2UnaryPostfixOperator(String textRepr) {
            this.textRepr = textRepr;
        }

        @Override
        public String getTextRepresentation() {
            return textRepr;
        }

        public static H2UnaryPostfixOperator getRandom() {
            return Randomly.fromOptions(values());
        }

    }

    public enum H2UnaryPrefixOperator implements Operator {

        NOT("NOT"), PLUS("+"), MINUS("-");

        private String textRepr;

        H2UnaryPrefixOperator(String textRepr) {
            this.textRepr = textRepr;
        }

        @Override
        public String getTextRepresentation() {
            return textRepr;
        }

        public static H2UnaryPrefixOperator getRandom() {
            return Randomly.fromOptions(values());
        }

    }

    public enum H2BinaryArithmeticOperator implements Operator {
        CONCAT("||"), ADD("+"), SUB("-"), MULT("*"), DIV("/"), MOD("%");

        private String textRepr;

        H2BinaryArithmeticOperator(String textRepr) {
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

    public enum H2BinaryLogicalOperator implements Operator {

        AND, OR;

        @Override
        public String getTextRepresentation() {
            return toString();
        }

        public static Operator getRandom() {
            return Randomly.fromOptions(values());
        }

    }

    public enum H2BinaryComparisonOperator implements Operator {

        EQUALS("="), GREATER(">"), GREATER_EQUALS(">="), SMALLER("<"), SMALLER_EQUALS("<="), NOT_EQUALS("!="),
        IS_DISTINCT_FROM("IS DISTINCT FROM"), IS_NOT_DISTINCT("IS NOT DISTINCT FROM"), LIKE("LIKE"),
        NOT_LIKE("NOT LIKE"), REGEXP("REGEXP"), NOT_REGEXP("NOT REGEXP");

        private String textRepr;

        H2BinaryComparisonOperator(String textRepr) {
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

    @Override
    public Node<H2Expression> negatePredicate(Node<H2Expression> predicate) {
        return new NewUnaryPrefixOperatorNode<>(predicate, H2UnaryPrefixOperator.NOT);
    }

    @Override
    public Node<H2Expression> isNull(Node<H2Expression> expr) {
        return new NewUnaryPostfixOperatorNode<>(expr, H2UnaryPostfixOperator.IS_NULL);
    }

}
