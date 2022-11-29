package sqlancer.hsqldb.gen;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.newast.NewBinaryOperatorNode;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.gen.TypedExpressionGenerator;
import sqlancer.hsqldb.HSQLDBProvider;
import sqlancer.hsqldb.HSQLDBSchema;
import sqlancer.hsqldb.ast.HSQLDBColumnReference;
import sqlancer.hsqldb.ast.HSQLDBConstant;
import sqlancer.hsqldb.ast.HSQLDBExpression;
import sqlancer.hsqldb.ast.HSQLDBUnaryPostfixOperation;
import sqlancer.hsqldb.ast.HSQLDBUnaryPrefixOperation;

public final class HSQLDBExpressionGenerator extends
        TypedExpressionGenerator<Node<HSQLDBExpression>, HSQLDBSchema.HSQLDBColumn, HSQLDBSchema.HSQLDBCompositeDataType> {

    private enum Expression {
        BINARY_LOGICAL, BINARY_COMPARISON, BINARY_ARITHMETIC;
    }

    HSQLDBProvider.HSQLDBGlobalState hsqldbGlobalState;

    public HSQLDBExpressionGenerator(HSQLDBProvider.HSQLDBGlobalState globalState) {
        this.hsqldbGlobalState = globalState;
    }

    @Override
    public Node<HSQLDBExpression> generatePredicate() {
        return generateExpression(
                HSQLDBSchema.HSQLDBCompositeDataType.getRandomWithType(HSQLDBSchema.HSQLDBDataType.BOOLEAN));
    }

    @Override
    public Node<HSQLDBExpression> negatePredicate(Node<HSQLDBExpression> predicate) {
        return new HSQLDBUnaryPrefixOperation(HSQLDBUnaryPrefixOperation.HSQLDBUnaryPrefixOperator.NOT, predicate);
    }

    @Override
    public Node<HSQLDBExpression> isNull(Node<HSQLDBExpression> expr) {
        return new HSQLDBUnaryPostfixOperation(expr, HSQLDBUnaryPostfixOperation.HSQLDBUnaryPostfixOperator.IS_NULL);
    }

    @Override
    public Node<HSQLDBExpression> generateConstant(HSQLDBSchema.HSQLDBCompositeDataType type) {
        switch (type.getType()) {
        case NULL:
            return HSQLDBConstant.createNullConstant();
        case CHAR:
            return HSQLDBConstant.HSQLDBTextConstant
                    .createStringConstant(hsqldbGlobalState.getRandomly().getAlphabeticChar(), type.getSize());
        case VARCHAR:
            return HSQLDBConstant.HSQLDBTextConstant.createStringConstant(hsqldbGlobalState.getRandomly().getString(),
                    type.getSize());
        case TIME:
            return HSQLDBConstant.createTimeConstant(
                    hsqldbGlobalState.getRandomly().getLong(0, System.currentTimeMillis()), type.getSize());
        case TIMESTAMP:
            return HSQLDBConstant.createTimestampConstant(
                    hsqldbGlobalState.getRandomly().getLong(0, System.currentTimeMillis()), type.getSize());

        case INTEGER:
            return HSQLDBConstant.HSQLDBIntConstant.createIntConstant(Randomly.getNonCachedInteger());
        case DOUBLE:
            return HSQLDBConstant.HSQLDBDoubleConstant.createFloatConstant(hsqldbGlobalState.getRandomly().getDouble());
        case BOOLEAN:
            return HSQLDBConstant.HSQLDBBooleanConstant.createBooleanConstant(Randomly.getBoolean());
        case DATE:
            return HSQLDBConstant
                    .createDateConstant(hsqldbGlobalState.getRandomly().getLong(0, System.currentTimeMillis()));
        case BINARY:
            return HSQLDBConstant.createBinaryConstant(Randomly.getNonCachedInteger(), type.getSize());
        default:
            throw new AssertionError("Unknown type: " + type);
        }
    }

    @Override
    protected Node<HSQLDBExpression> generateExpression(HSQLDBSchema.HSQLDBCompositeDataType type, int depth) {
        if (depth >= hsqldbGlobalState.getOptions().getMaxExpressionDepth()
                || Randomly.getBooleanWithSmallProbability()) {
            return generateLeafNode(type);
        }

        List<HSQLDBExpressionGenerator.Expression> possibleOptions = new ArrayList<>(
                Arrays.asList(HSQLDBExpressionGenerator.Expression.values()));

        HSQLDBExpressionGenerator.Expression expr = Randomly.fromList(possibleOptions);
        BinaryOperatorNode.Operator op;
        switch (expr) {
        case BINARY_LOGICAL:
        case BINARY_ARITHMETIC:
            op = HSQLDBExpressionGenerator.HSQLDBBinaryLogicalOperator.getRandom();
            break;
        case BINARY_COMPARISON:
            op = HSQLDBDBBinaryComparisonOperator.getRandom();
            break;
        default:
            throw new AssertionError();
        }

        return new NewBinaryOperatorNode<>(generateExpression(type, depth + 1), generateExpression(type, depth + 1),
                op);

    }

    @Override
    protected Node<HSQLDBExpression> generateColumn(HSQLDBSchema.HSQLDBCompositeDataType type) {
        HSQLDBSchema.HSQLDBColumn column = Randomly
                .fromList(columns.stream().filter(c -> c.getType() == type).collect(Collectors.toList()));
        return new HSQLDBColumnReference(column);
    }

    @Override
    protected HSQLDBSchema.HSQLDBCompositeDataType getRandomType() {
        return HSQLDBSchema.HSQLDBCompositeDataType.getRandomWithoutNull();
    }

    @Override
    protected boolean canGenerateColumnOfType(HSQLDBSchema.HSQLDBCompositeDataType type) {
        return columns.stream().anyMatch(c -> c.getType() == type);
    }

    public enum HSQLDBBinaryLogicalOperator implements BinaryOperatorNode.Operator {

        AND, OR;

        @Override
        public String getTextRepresentation() {
            return toString();
        }

        public static BinaryOperatorNode.Operator getRandom() {
            return Randomly.fromOptions(values());
        }

    }

    public enum HSQLDBDBBinaryComparisonOperator implements BinaryOperatorNode.Operator {
        EQUALS("="), GREATER(">"), GREATER_EQUALS(">="), SMALLER("<"), SMALLER_EQUALS("<="), NOT_EQUALS("!=");

        private String textRepr;

        HSQLDBDBBinaryComparisonOperator(String textRepr) {
            this.textRepr = textRepr;
        }

        public static BinaryOperatorNode.Operator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return textRepr;
        }

    }

    public enum HSQLDBDBBinaryArithmeticOperator implements BinaryOperatorNode.Operator {
        CONCAT("||"), ADD("+"), SUB("-"), MULT("*"), DIV("/"), MOD("%"), AND("&"), OR("|"), LSHIFT("<<"), RSHIFT(">>");

        private String textRepr;

        HSQLDBDBBinaryArithmeticOperator(String textRepr) {
            this.textRepr = textRepr;
        }

        public static BinaryOperatorNode.Operator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return textRepr;
        }

    }

    @Override
    public List<Node<HSQLDBExpression>> generateOrderBys() {
        List<Node<HSQLDBExpression>> expressions = new ArrayList<>();
        int nr = Randomly.smallNumber() + 1;
        ArrayList<HSQLDBSchema.HSQLDBColumn> hsqldbColumns = new ArrayList<>(columns);
        for (int i = 0; i < nr && !hsqldbColumns.isEmpty(); i++) {
            HSQLDBSchema.HSQLDBColumn randomColumn = Randomly.fromList(hsqldbColumns);
            HSQLDBColumnReference columnReference = new HSQLDBColumnReference(randomColumn);
            hsqldbColumns.remove(randomColumn);
            expressions.add(columnReference);
        }
        return expressions;
    }
}
