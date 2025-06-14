package sqlancer.questdb.gen;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.Randomly.StringGenerationStrategy;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.common.gen.UntypedExpressionGenerator;
import sqlancer.questdb.QuestDBProvider.QuestDBGlobalState;
import sqlancer.questdb.QuestDBSchema.QuestDBColumn;
import sqlancer.questdb.QuestDBSchema.QuestDBDataType;
import sqlancer.questdb.ast.QuestDBBinaryOperation;
import sqlancer.questdb.ast.QuestDBColumnReference;
import sqlancer.questdb.ast.QuestDBConstant;
import sqlancer.questdb.ast.QuestDBExpression;
import sqlancer.questdb.ast.QuestDBInOperation;
import sqlancer.questdb.ast.QuestDBUnaryPostfixOperation;
import sqlancer.questdb.ast.QuestDBUnaryPrefixOperation;

public class QuestDBExpressionGenerator extends UntypedExpressionGenerator<QuestDBExpression, QuestDBColumn> {

    private final QuestDBGlobalState globalState;

    public QuestDBExpressionGenerator(QuestDBGlobalState globalState) {
        this.globalState = globalState;
    }

    private enum Expression {
        UNARY_POSTFIX, UNARY_PREFIX, BINARY_COMPARISON, BINARY_LOGICAL, BINARY_ARITHMETIC, IN
    }

    @Override
    public QuestDBExpression negatePredicate(QuestDBExpression predicate) {
        return new QuestDBUnaryPrefixOperation(predicate, QuestDBUnaryPrefixOperator.NOT);
    }

    @Override
    public QuestDBExpression isNull(QuestDBExpression expr) {
        return new QuestDBUnaryPostfixOperation(expr, QuestDBUnaryPostfixOperator.IS_NULL);
    }

    @Override
    public QuestDBExpression generateConstant() {
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
        case SYMBOL:
            StringGenerationStrategy strategy = Randomly.StringGenerationStrategy.ALPHANUMERIC;
            return QuestDBConstant.createSymbolConstant(strategy.getString(globalState.getRandomly(), 0));
        // case CHAR:
        // case DATE:
        // case TIMESTAMP:
        // throw new IgnoreMeException();
        default:
            throw new AssertionError("Unknown type: " + type);
        }
    }

    @Override
    protected QuestDBExpression generateExpression(int depth) {
        if (depth >= globalState.getOptions().getMaxExpressionDepth() || Randomly.getBoolean()) {
            return generateLeafNode();
        }

        List<Expression> possibleOptions = new ArrayList<>(Arrays.asList(Expression.values()));
        Expression expr = Randomly.fromList(possibleOptions);

        switch (expr) {
        case UNARY_PREFIX:
            return new QuestDBUnaryPrefixOperation(generateExpression(depth + 1),
                    QuestDBUnaryPrefixOperator.getRandom());
        case UNARY_POSTFIX:
            return new QuestDBUnaryPostfixOperation(generateExpression(depth + 1),
                    QuestDBUnaryPostfixOperator.getRandom());
        case BINARY_COMPARISON:
            return new QuestDBBinaryOperation(generateExpression(depth + 1), generateExpression(depth + 1),
                    QuestDBBinaryComparisonOperator.getRandom());
        case BINARY_ARITHMETIC:
            return new QuestDBBinaryOperation(generateExpression(depth + 1), generateExpression(depth + 1),
                    QuestDBBinaryArithmeticOperator.getRandom());
        case BINARY_LOGICAL:
            return new QuestDBBinaryOperation(generateExpression(depth + 1), generateExpression(depth + 1),
                    QuestDBBinaryLogicalOperator.getRandom());
        case IN:
            return new QuestDBInOperation(generateExpression(depth + 1),
                    generateExpressions(Randomly.smallNumber() + 1, depth + 1), Randomly.getBoolean());
        default:
            throw new AssertionError("Expression generation failed, depth=" + depth);
        }
    }

    @Override
    protected QuestDBExpression generateColumn() {
        QuestDBColumn column = Randomly.fromList(columns);
        return new QuestDBColumnReference(column);
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
        NOT_EQUALS("!="), REGEX_POSIX("~"), REGEX_POSIT_NOT("!~");

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

    public enum QuestDBBinaryArithmeticOperator implements Operator {
        CONCAT("||"), ADD("+"), SUB("-"), MULT("*"), DIV("/"), MOD("%"), AND("&"), OR("|"); // , LSHIFT("<<"),
                                                                                            // RSHIFT(">>");

        private String textRepr;

        QuestDBBinaryArithmeticOperator(String textRepr) {
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
