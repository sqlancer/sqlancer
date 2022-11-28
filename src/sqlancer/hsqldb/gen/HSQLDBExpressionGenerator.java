package sqlancer.hsqldb.gen;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.newast.NewBinaryOperatorNode;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.gen.TypedExpressionGenerator;
import sqlancer.hsqldb.HSQLDBProvider;
import sqlancer.hsqldb.HSQLDBSchema;
import sqlancer.hsqldb.ast.HSQLDBConstant;
import sqlancer.hsqldb.ast.HSQLDBExpression;
import sqlancer.hsqldb.ast.HSQLDBUnaryPostfixOperation;
import sqlancer.hsqldb.ast.HSQLDBUnaryPrefixOperation;

public final class HSQLDBExpressionGenerator extends
        TypedExpressionGenerator<Node<HSQLDBExpression>, HSQLDBSchema.HSQLDBColumn, HSQLDBSchema.HSQLDBCompositeDataType> {

    private enum Expression {
        BINARY_LOGICAL;
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
        if (type.getType() == HSQLDBSchema.HSQLDBDataType.NULL || Randomly.getBooleanWithSmallProbability()) {
            return HSQLDBConstant.createNullConstant();
        }
        switch (type.getType()) {
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
        if (depth >= hsqldbGlobalState.getOptions().getMaxExpressionDepth() || Randomly.getBoolean()) {
            return generateLeafNode(type);
        }

        List<HSQLDBExpressionGenerator.Expression> possibleOptions = new ArrayList<>(
                Arrays.asList(HSQLDBExpressionGenerator.Expression.values()));

        HSQLDBExpressionGenerator.Expression expr = Randomly.fromList(possibleOptions);
        switch (expr) {
        case BINARY_LOGICAL:
            BinaryOperatorNode.Operator op = HSQLDBExpressionGenerator.HSQLDBBinaryLogicalOperator.getRandom();
            return new NewBinaryOperatorNode<>(generateExpression(type), generateExpression(type), op);
        default:
            throw new AssertionError();
        }
    }

    @Override
    protected Node<HSQLDBExpression> generateColumn(HSQLDBSchema.HSQLDBCompositeDataType type) {
        return null;
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
}
