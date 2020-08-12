package sqlancer.h2;

import java.util.Random;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.common.ast.newast.ColumnReferenceNode;
import sqlancer.common.ast.newast.NewBinaryOperatorNode;
import sqlancer.common.ast.newast.NewUnaryPostfixOperatorNode;
import sqlancer.common.ast.newast.NewUnaryPrefixOperatorNode;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.gen.UntypedExpressionGenerator;
import sqlancer.h2.H2Provider.H2GlobalState;
import sqlancer.h2.H2Schema.H2Column;
import sqlancer.h2.H2Schema.H2DataType;

public class H2ExpressionGenerator extends UntypedExpressionGenerator<Node<H2Expression>, H2Column> {

    private static final Random RANDOM = new Random();

    private final H2GlobalState globalState;

    public H2ExpressionGenerator(H2GlobalState globalState) {
        this.globalState = globalState;
    }

    private enum Expression {
        BINARY_COMPARISON, BINARY_LOGICAL, UNARY_POSTFIX, UNARY_PREFIX;
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
        default:
            throw new AssertionError();
        }
    }

    public static int getUncachedInt() {
        return RANDOM.nextInt();
    }

    public enum H2UnaryPostfixOperator implements Operator {

        IS_NULL("IS NULL"), IS_NOT_NULL("IS NOT NULL");

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

        EQUALS("="), GREATER(">"), GREATER_EQUALS(">="), SMALLER("<"), SMALLER_EQUALS("<="), NOT_EQUALS("!=");

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
