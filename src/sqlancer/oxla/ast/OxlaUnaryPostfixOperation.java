package sqlancer.oxla.ast;

import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.newast.NewUnaryPostfixOperatorNode;
import sqlancer.oxla.schema.OxlaDataType;

import java.util.List;

public class OxlaUnaryPostfixOperation extends NewUnaryPostfixOperatorNode<OxlaExpression>
        implements OxlaExpression {
    public OxlaUnaryPostfixOperation(OxlaExpression expr, BinaryOperatorNode.Operator op) {
        super(expr, op);
    }

    @Override
    public OxlaConstant getExpectedValue() {
        OxlaConstant expectedValue = getExpr().getExpectedValue();
        if (expectedValue == null) {
            return null;
        }
        return ((OxlaUnaryPostfixOperation.OxlaUnaryPostfixOperator) op).apply(new OxlaConstant[]{expectedValue});
    }

    public static class OxlaUnaryPostfixOperator extends OxlaOperator {
        private final OxlaApplyFunction applyFunction;

        public OxlaUnaryPostfixOperator(String textRepresentation, OxlaTypeOverload overload, OxlaApplyFunction applyFunction) {
            super(textRepresentation, overload);
            this.applyFunction = applyFunction;
        }

        public OxlaConstant apply(OxlaConstant[] constants) {
            if (constants.length != 1) {
                throw new AssertionError(String.format("OxlaUnaryPrefixOperation::apply* failed: expected 1 argument, but got %d", constants.length));
            }
            return applyFunction.apply(constants);
        }
    }

    // FIXME Imho, this class shouldn't hardcode the operators, but query the database for them instead.
    //       Sadly, Oxla does not support every pg_* table in the metastore so it's impossible for now.
    public static final List<OxlaOperator> ALL = List.of(
            new OxlaUnaryPostfixOperator("IS NULL", new OxlaTypeOverload(OxlaDataType.BOOLEAN, OxlaDataType.BOOLEAN), OxlaUnaryPostfixOperation::applyIsNull),
            new OxlaUnaryPostfixOperator("IS NULL", new OxlaTypeOverload(OxlaDataType.DATE, OxlaDataType.BOOLEAN), OxlaUnaryPostfixOperation::applyIsNull),
            new OxlaUnaryPostfixOperator("IS NULL", new OxlaTypeOverload(OxlaDataType.FLOAT32, OxlaDataType.BOOLEAN), OxlaUnaryPostfixOperation::applyIsNull),
            new OxlaUnaryPostfixOperator("IS NULL", new OxlaTypeOverload(OxlaDataType.FLOAT64, OxlaDataType.BOOLEAN), OxlaUnaryPostfixOperation::applyIsNull),
            new OxlaUnaryPostfixOperator("IS NULL", new OxlaTypeOverload(OxlaDataType.INT32, OxlaDataType.BOOLEAN), OxlaUnaryPostfixOperation::applyIsNull),
            new OxlaUnaryPostfixOperator("IS NULL", new OxlaTypeOverload(OxlaDataType.INT64, OxlaDataType.BOOLEAN), OxlaUnaryPostfixOperation::applyIsNull),
            new OxlaUnaryPostfixOperator("IS NULL", new OxlaTypeOverload(OxlaDataType.INTERVAL, OxlaDataType.BOOLEAN), OxlaUnaryPostfixOperation::applyIsNull),
            new OxlaUnaryPostfixOperator("IS NULL", new OxlaTypeOverload(OxlaDataType.JSON, OxlaDataType.BOOLEAN), OxlaUnaryPostfixOperation::applyIsNull),
            new OxlaUnaryPostfixOperator("IS NULL", new OxlaTypeOverload(OxlaDataType.TEXT, OxlaDataType.BOOLEAN), OxlaUnaryPostfixOperation::applyIsNull),
            new OxlaUnaryPostfixOperator("IS NULL", new OxlaTypeOverload(OxlaDataType.TIME, OxlaDataType.BOOLEAN), OxlaUnaryPostfixOperation::applyIsNull),
            new OxlaUnaryPostfixOperator("IS NULL", new OxlaTypeOverload(OxlaDataType.TIMESTAMP, OxlaDataType.BOOLEAN), OxlaUnaryPostfixOperation::applyIsNull),
            new OxlaUnaryPostfixOperator("IS NULL", new OxlaTypeOverload(OxlaDataType.TIMESTAMPTZ, OxlaDataType.BOOLEAN), OxlaUnaryPostfixOperation::applyIsNull),
            new OxlaUnaryPostfixOperator("IS NOT NULL", new OxlaTypeOverload(OxlaDataType.BOOLEAN, OxlaDataType.BOOLEAN), OxlaUnaryPostfixOperation::applyIsNotNull),
            new OxlaUnaryPostfixOperator("IS NOT NULL", new OxlaTypeOverload(OxlaDataType.DATE, OxlaDataType.BOOLEAN), OxlaUnaryPostfixOperation::applyIsNotNull),
            new OxlaUnaryPostfixOperator("IS NOT NULL", new OxlaTypeOverload(OxlaDataType.FLOAT32, OxlaDataType.BOOLEAN), OxlaUnaryPostfixOperation::applyIsNotNull),
            new OxlaUnaryPostfixOperator("IS NOT NULL", new OxlaTypeOverload(OxlaDataType.FLOAT64, OxlaDataType.BOOLEAN), OxlaUnaryPostfixOperation::applyIsNotNull),
            new OxlaUnaryPostfixOperator("IS NOT NULL", new OxlaTypeOverload(OxlaDataType.INT32, OxlaDataType.BOOLEAN), OxlaUnaryPostfixOperation::applyIsNotNull),
            new OxlaUnaryPostfixOperator("IS NOT NULL", new OxlaTypeOverload(OxlaDataType.INT64, OxlaDataType.BOOLEAN), OxlaUnaryPostfixOperation::applyIsNotNull),
            new OxlaUnaryPostfixOperator("IS NOT NULL", new OxlaTypeOverload(OxlaDataType.INTERVAL, OxlaDataType.BOOLEAN), OxlaUnaryPostfixOperation::applyIsNotNull),
            new OxlaUnaryPostfixOperator("IS NOT NULL", new OxlaTypeOverload(OxlaDataType.JSON, OxlaDataType.BOOLEAN), OxlaUnaryPostfixOperation::applyIsNotNull),
            new OxlaUnaryPostfixOperator("IS NOT NULL", new OxlaTypeOverload(OxlaDataType.TEXT, OxlaDataType.BOOLEAN), OxlaUnaryPostfixOperation::applyIsNotNull),
            new OxlaUnaryPostfixOperator("IS NOT NULL", new OxlaTypeOverload(OxlaDataType.TIME, OxlaDataType.BOOLEAN), OxlaUnaryPostfixOperation::applyIsNotNull),
            new OxlaUnaryPostfixOperator("IS NOT NULL", new OxlaTypeOverload(OxlaDataType.TIMESTAMP, OxlaDataType.BOOLEAN), OxlaUnaryPostfixOperation::applyIsNotNull),
            new OxlaUnaryPostfixOperator("IS NOT NULL", new OxlaTypeOverload(OxlaDataType.TIMESTAMPTZ, OxlaDataType.BOOLEAN), OxlaUnaryPostfixOperation::applyIsNotNull),
            new OxlaUnaryPostfixOperator("IS TRUE", new OxlaTypeOverload(OxlaDataType.BOOLEAN, OxlaDataType.BOOLEAN), (c) -> applyIsBoolean(c, false, true)),
            new OxlaUnaryPostfixOperator("IS NOT TRUE", new OxlaTypeOverload(OxlaDataType.BOOLEAN, OxlaDataType.BOOLEAN), (c) -> applyIsBoolean(c, true, true)),
            new OxlaUnaryPostfixOperator("IS FALSE", new OxlaTypeOverload(OxlaDataType.BOOLEAN, OxlaDataType.BOOLEAN), (c) -> applyIsBoolean(c, false, false)),
            new OxlaUnaryPostfixOperator("IS NOT FALSE", new OxlaTypeOverload(OxlaDataType.BOOLEAN, OxlaDataType.BOOLEAN), (c) -> applyIsBoolean(c, true, false))
    );
    public static final OxlaOperator IS_NULL = ALL.get(0);
    public static final OxlaOperator IS_TRUE = ALL.get(24);
    public static final OxlaOperator IS_FALSE = ALL.get(26);

    private static OxlaConstant applyIsNull(OxlaConstant[] constants) {
        return OxlaConstant.createBooleanConstant(constants[0] instanceof OxlaConstant.OxlaNullConstant);
    }

    private static OxlaConstant applyIsNotNull(OxlaConstant[] constants) {
        return OxlaConstant.createBooleanConstant(!(constants[0] instanceof OxlaConstant.OxlaNullConstant));
    }

    private static OxlaConstant applyIsBoolean(OxlaConstant[] constants, boolean isNot, boolean isTrue) {
        final OxlaConstant constant = constants[0];
        if (constant instanceof OxlaConstant.OxlaNullConstant) {
            return OxlaConstant.createBooleanConstant(isNot);
        } else if (constant instanceof OxlaConstant.OxlaBooleanConstant) {
            return OxlaConstant.createBooleanConstant(((OxlaConstant.OxlaBooleanConstant) constant).value == isTrue);
        }
        throw new AssertionError(String.format("OxlaUnaryPrefixOperation::applyIsBoolean failed: %s", constant.getClass()));
    }
}
