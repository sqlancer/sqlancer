package sqlancer.hsqldb.gen;

import sqlancer.Randomly;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.gen.TypedExpressionGenerator;
import sqlancer.hsqldb.HSQLDBProvider;
import sqlancer.hsqldb.HSQLDBSchema;
import sqlancer.hsqldb.ast.HSQLDBConstant;
import sqlancer.hsqldb.ast.HSQLDBExpression;


public final class HSQLDBExpressionGenerator extends TypedExpressionGenerator<Node<HSQLDBExpression>, HSQLDBSchema.HSQLDBColumn, HSQLDBSchema.HSQLDBCompositeDataType> {

    HSQLDBProvider.HSQLDBGlobalState hsqldbGlobalState;
    public HSQLDBExpressionGenerator(HSQLDBProvider.HSQLDBGlobalState globalState) {
        this.hsqldbGlobalState = globalState;
    }

    @Override
    public Node<HSQLDBExpression> generatePredicate() {
        return null;
    }

    @Override
    public Node<HSQLDBExpression> negatePredicate(Node<HSQLDBExpression> predicate) {
        return null;
    }

    @Override
    public Node<HSQLDBExpression> isNull(Node<HSQLDBExpression> expr) {
        return null;
    }

    @Override
    public Node<HSQLDBExpression> generateConstant(HSQLDBSchema.HSQLDBCompositeDataType type) {
        if (type.getType() == HSQLDBSchema.HSQLDBDataType.NULL || Randomly.getBooleanWithSmallProbability()) {
            return HSQLDBConstant.createNullConstant();
        }
        switch (type.getType()) {
            case CHAR:
                return HSQLDBConstant.HSQLDBTextConstant.createStringConstant(hsqldbGlobalState.getRandomly().getAlphabeticChar(), type.getSize());
            case VARCHAR:
                return HSQLDBConstant.HSQLDBTextConstant.createStringConstant(hsqldbGlobalState.getRandomly().getString(), type.getSize());
            case TIME:
                return HSQLDBConstant.createTimeConstant(hsqldbGlobalState.getRandomly().getLong(0, System.currentTimeMillis()), type.getSize());
            case TIMESTAMP:
                return HSQLDBConstant.createTimestampConstant(hsqldbGlobalState.getRandomly().getLong(0, System.currentTimeMillis()), type.getSize() );

            case INTEGER:
                return HSQLDBConstant.HSQLDBIntConstant.createIntConstant(Randomly.getNonCachedInteger());
            case DOUBLE:
                return HSQLDBConstant.HSQLDBDoubleConstant.createFloatConstant(hsqldbGlobalState.getRandomly().getDouble());
            case BOOLEAN:
                return HSQLDBConstant.HSQLDBBooleanConstant.createBooleanConstant(Randomly.getBoolean());
            case DATE:
                return HSQLDBConstant.createDateConstant(hsqldbGlobalState.getRandomly().getLong(0, System.currentTimeMillis()));
            case BINARY:
                return HSQLDBConstant.createBinaryConstant(Randomly.getNonCachedInteger(), type.getSize());
//            case OTHER:
//                break;
//            case NULL:
//                break;
            default:
                throw new AssertionError("Unknown type: " + type);
        }
    }

    @Override
    protected Node<HSQLDBExpression> generateExpression(HSQLDBSchema.HSQLDBCompositeDataType type, int depth) {
        return null;
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

}
