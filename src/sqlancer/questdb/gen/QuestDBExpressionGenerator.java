package sqlancer.questdb.gen;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.common.ast.newast.ColumnReferenceNode;
import sqlancer.common.ast.newast.NewUnaryPostfixOperatorNode;
import sqlancer.common.ast.newast.NewUnaryPrefixOperatorNode;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.gen.UntypedExpressionGenerator;
import sqlancer.questdb.QuestDBProvider.QuestDBGlobalState;
import sqlancer.questdb.QuestDBSchema.QuestDBColumn;
import sqlancer.questdb.QuestDBSchema.QuestDBDataType;
import sqlancer.questdb.ast.QuestDBConstant;
import sqlancer.questdb.ast.QuestDBExpression;

public class QuestDBExpressionGenerator extends UntypedExpressionGenerator<Node<QuestDBExpression>, QuestDBColumn> {

    private final QuestDBGlobalState globalState;

    public QuestDBExpressionGenerator(QuestDBGlobalState globalState) {
        this.globalState = globalState;
    }

    @Override
    public Node<QuestDBExpression> negatePredicate(Node<QuestDBExpression> predicate) {
        return new NewUnaryPrefixOperatorNode<>(predicate, QuestDBUnaryPrefixOperator.NOT);
    }

    @Override
    public Node<QuestDBExpression> isNull(Node<QuestDBExpression> expr) {
        return new NewUnaryPostfixOperatorNode<>(expr, QuestDBUnaryPostfixOperator.IS_NULL);
    }

    @Override
    public Node<QuestDBExpression> generateConstant() {
        if (Randomly.getBooleanWithSmallProbability()) {
            return QuestDBConstant.createNullConstant();
        }
        QuestDBDataType type = QuestDBDataType.getRandomWithoutNull();
        switch (type) {
        case INT:
            return QuestDBConstant.createIntConstant(globalState.getRandomly().getInteger());
        case BOOLEAN:
            return QuestDBConstant.createBooleanConstant(Randomly.getBoolean());
        case FLOAT:
            return QuestDBConstant.createFloatConstant(globalState.getRandomly().getDouble());
        // case CHAR:
        // case DATE:
        // case TIMESTAMP:
        // throw new IgnoreMeException();
        default:
            throw new AssertionError("Unknown type: " + type);
        }
    }

    @Override
    protected Node<QuestDBExpression> generateExpression(int depth) {
        if (depth >= globalState.getOptions().getMaxExpressionDepth() || Randomly.getBoolean()) {
            return generateLeafNode();
        } else {
            throw new AssertionError("Expression generation failed, depth=" + depth);
        }
    }

    @Override
    protected Node<QuestDBExpression> generateColumn() {
        QuestDBColumn column = Randomly.fromList(columns);
        return new ColumnReferenceNode<>(column);
    }

    public enum QuestDBUnaryPostfixOperator implements Operator {
        IS_NULL("IS NULL"), IS_NOT_NULL("IS NOT NULL");

        private String textRepr;

        QuestDBUnaryPostfixOperator(String textRepr) {
            this.textRepr = textRepr;
        }

        @Override
        public String getTextRepresentation() {
            return textRepr;
        }

        public static QuestDBUnaryPostfixOperator getRandom() {
            return Randomly.fromOptions(values());
        }
    }

    public enum QuestDBUnaryPrefixOperator implements Operator {

        NOT("NOT");

        private String textRepr;

        QuestDBUnaryPrefixOperator(String textRepr) {
            this.textRepr = textRepr;
        }

        @Override
        public String getTextRepresentation() {
            return textRepr;
        }

        public static QuestDBUnaryPrefixOperator getRandom() {
            return Randomly.fromOptions(values());
        }

    }

    public enum QuestDBBinaryLogicalOperator implements Operator {

        AND, OR;

        @Override
        public String getTextRepresentation() {
            return toString();
        }

        public static Operator getRandom() {
            return Randomly.fromOptions(values());
        }

    }

    public enum QuestDBBinaryComparisonOperator implements Operator {
        EQUALS("="), GREATER_THAN(">"), GREATER_THAN_EQUALS(">="), LESS_THAN("<"), SMALLER_THAN_EQUALS("<="),
        NOT_EQUALS("!=");

        private String textRepr;

        QuestDBBinaryComparisonOperator(String textRepr) {
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
