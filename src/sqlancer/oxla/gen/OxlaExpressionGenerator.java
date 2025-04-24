package sqlancer.oxla.gen;

import sqlancer.Randomly;
import sqlancer.common.gen.TypedExpressionGenerator;
import sqlancer.oxla.OxlaGlobalState;
import sqlancer.oxla.ast.*;
import sqlancer.oxla.schema.OxlaColumn;
import sqlancer.oxla.schema.OxlaDataType;

import java.util.ArrayList;
import java.util.List;

public class OxlaExpressionGenerator extends TypedExpressionGenerator<OxlaExpression, OxlaColumn, OxlaDataType> {
    private final OxlaGlobalState globalState;
    private final Randomly randomly;

    public OxlaExpressionGenerator(OxlaGlobalState globalState) {
        this.globalState = globalState;
        this.randomly = globalState.getRandomly();
    }

    private enum ExpressionType {
        UNARY_PREFIX, UNARY_POSTFIX;

        public static ExpressionType getRandom() {
            return Randomly.fromOptions(values());
        }
    }

    @Override
    public OxlaExpression generateConstant(OxlaDataType type) {
        if (Randomly.getBooleanWithSmallProbability()) {
            return OxlaConstant.createNullConstant();
        }
        switch (type) {
            case BOOLEAN:
                return OxlaConstant.createBooleanConstant(Randomly.getBoolean());
            case DATE:
                return OxlaConstant.createDateConstant(randomly.getInteger32());
            case FLOAT32:
                return OxlaConstant.createFloat32Constant(randomly.getFloat());
            case FLOAT64:
                return OxlaConstant.createFloat64Constant(randomly.getDouble());
            case INT32:
                return OxlaConstant.createInt32Constant(randomly.getInteger32());
            case INT64:
                return OxlaConstant.createInt64Constant(randomly.getLong());
            case INTERVAL:
                return OxlaConstant.createIntervalConstant(randomly.getInteger32(), randomly.getInteger32(), randomly.getLong());
            case JSON:
                return OxlaConstant.createJsonConstant(randomly.getString());
            case TEXT:
                return OxlaConstant.createTextConstant(randomly.getString());
            case TIME:
                return OxlaConstant.createTimeConstant(randomly.getInteger32());
            case TIMESTAMP:
                return OxlaConstant.createTimestampConstant(randomly.getInteger());
            case TIMESTAMPTZ:
                return OxlaConstant.createTimestamptzConstant(randomly.getInteger());
            default:
                throw new AssertionError(type);
        }
    }

    @Override
    protected OxlaExpression generateExpression(OxlaDataType wantReturnType, int depth) {
        if (depth >= globalState.getOptions().getMaxExpressionDepth() || Randomly.getBoolean()) {
            return generateLeafNode(wantReturnType);
        }

        ExpressionType expressionType = ExpressionType.getRandom();
        switch (expressionType) {
            case UNARY_PREFIX:
                return generateOperator(OxlaUnaryPrefixOperation.ALL, wantReturnType, depth);
            case UNARY_POSTFIX:
                return generateOperator(OxlaUnaryPostfixOperation.ALL, wantReturnType, depth);
            default:
                throw new AssertionError(expressionType);
        }
    }

    @Override
    protected OxlaExpression generateColumn(OxlaDataType type) {
        return (OxlaExpression) Randomly.fromList(columns);
    }

    @Override
    protected OxlaDataType getRandomType() {
        return OxlaDataType.getRandomType();
    }

    @Override
    protected boolean canGenerateColumnOfType(OxlaDataType type) {
        return columns.stream().anyMatch(column -> column.getType() == type);
    }

    @Override
    public OxlaExpression generatePredicate() {
        return generateExpression(OxlaDataType.BOOLEAN);
    }

    @Override
    public OxlaExpression negatePredicate(OxlaExpression predicate) {
        return new OxlaUnaryPrefixOperation(predicate, OxlaUnaryPrefixOperation.NOT);
    }

    @Override
    public OxlaExpression isNull(OxlaExpression expr) {
        return new OxlaUnaryPostfixOperation(expr, OxlaUnaryPostfixOperation.IS_NULL);
    }

    private OxlaExpression generateOperator(List<OxlaOperator> operators, OxlaDataType wantReturnType, int depth) {
        List<OxlaOperator> validOperators = new ArrayList<>(operators);
        validOperators.removeIf(operator -> operator.overload.returnType != wantReturnType);

        if (validOperators.isEmpty()) {
            // In case no operator matches the criteria - we can safely generate a leaf expression instead.
            return generateLeafNode(wantReturnType);
        }

        OxlaOperator randomOperator = Randomly.fromList(validOperators);
        OxlaExpression inputExpression = generateExpression(randomOperator.overload.inputTypes[0], depth + 1);
        return new OxlaUnaryPrefixOperation(inputExpression, randomOperator);
    }
}
