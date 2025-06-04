package sqlancer.oxla.ast;

import sqlancer.IgnoreMeException;
import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.newast.NewBinaryOperatorNode;
import sqlancer.oxla.OxlaToStringVisitor;
import sqlancer.oxla.schema.OxlaDataType;

import java.util.List;

public class OxlaBinaryOperation extends NewBinaryOperatorNode<OxlaExpression>
        implements OxlaExpression {
    public OxlaBinaryOperation(OxlaExpression left, OxlaExpression right, BinaryOperatorNode.Operator op) {
        super(left, right, op);
    }

    @Override
    public OxlaConstant getExpectedValue() {
        OxlaConstant leftValue = getLeft().getExpectedValue();
        OxlaConstant rightValue = getRight().getExpectedValue();
        if (leftValue == null || rightValue == null) {
            return null;
        }
        if (leftValue instanceof OxlaConstant.OxlaNullConstant || rightValue instanceof OxlaConstant.OxlaNullConstant) {
            return OxlaConstant.createNullConstant();
        }
        return ((OxlaBinaryOperation.OxlaBinaryOperator) op).apply(new OxlaConstant[]{leftValue, rightValue});
    }

    @Override
    public String toString() {
        return OxlaToStringVisitor.asString(this);
    }

    public static class OxlaBinaryOperator extends OxlaOperator {
        private final OxlaApplyFunction applyFunction;

        public OxlaBinaryOperator(String textRepresentation, OxlaTypeOverload overload, OxlaApplyFunction applyFunction) {
            super(textRepresentation, overload);
            this.applyFunction = applyFunction;
        }

        public OxlaConstant apply(OxlaConstant[] constants) {
            if (applyFunction == null) {
                // NOTE: `applyFunction` is not implemented, thus PQS oracle should ignore this operator.
                throw new IgnoreMeException();
            }
            if (constants.length != 2) {
                throw new AssertionError(String.format("OxlaUnaryBinaryOperation::apply* failed: expected 2 arguments, but got %d", constants.length));
            }
            return applyFunction.apply(constants);
        }
    }

    public static final List<OxlaOperator> COMPARISON = List.of(
            // Less
            new OxlaBinaryOperator("<", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.INT64, OxlaDataType.INT64}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyLess),
            new OxlaBinaryOperator("<", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.INT64, OxlaDataType.INT32}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyLess),
            new OxlaBinaryOperator("<", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.BOOLEAN, OxlaDataType.BOOLEAN}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyLess),
            new OxlaBinaryOperator("<", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.DATE, OxlaDataType.DATE}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyLess),
            new OxlaBinaryOperator("<", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.DATE, OxlaDataType.TIMESTAMP}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyLess),
            new OxlaBinaryOperator("<", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.FLOAT64, OxlaDataType.FLOAT64}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyLess),
            new OxlaBinaryOperator("<", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.FLOAT64, OxlaDataType.FLOAT32}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyLess),
            new OxlaBinaryOperator("<", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.INT32, OxlaDataType.INT64}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyLess),
            new OxlaBinaryOperator("<", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.INT32, OxlaDataType.INT32}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyLess),
            new OxlaBinaryOperator("<", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.INTERVAL, OxlaDataType.INTERVAL}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyLess),
            new OxlaBinaryOperator("<", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.FLOAT32, OxlaDataType.FLOAT64}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyLess),
            new OxlaBinaryOperator("<", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.FLOAT32, OxlaDataType.FLOAT32}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyLess),
            new OxlaBinaryOperator("<", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.TEXT, OxlaDataType.TEXT}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyLess),
            new OxlaBinaryOperator("<", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.TIMESTAMP, OxlaDataType.DATE}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyLess),
            new OxlaBinaryOperator("<", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.TIMESTAMP, OxlaDataType.TIMESTAMP}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyLess),
            new OxlaBinaryOperator("<", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.TIMESTAMP, OxlaDataType.TIMESTAMPTZ}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyLess),
            new OxlaBinaryOperator("<", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.TIMESTAMPTZ, OxlaDataType.TIMESTAMP}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyLess),
            new OxlaBinaryOperator("<", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.TIMESTAMPTZ, OxlaDataType.TIMESTAMPTZ}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyLess),
            new OxlaBinaryOperator("<", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.TIME, OxlaDataType.TIME}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyLess),
            // Less Equal
            new OxlaBinaryOperator("<=", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.INT64, OxlaDataType.INT64}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyLessEqual),
            new OxlaBinaryOperator("<=", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.INT64, OxlaDataType.INT32}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyLessEqual),
            new OxlaBinaryOperator("<=", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.BOOLEAN, OxlaDataType.BOOLEAN}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyLessEqual),
            new OxlaBinaryOperator("<=", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.DATE, OxlaDataType.DATE}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyLessEqual),
            new OxlaBinaryOperator("<=", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.DATE, OxlaDataType.TIMESTAMP}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyLessEqual),
            new OxlaBinaryOperator("<=", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.FLOAT64, OxlaDataType.FLOAT64}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyLessEqual),
            new OxlaBinaryOperator("<=", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.FLOAT64, OxlaDataType.FLOAT32}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyLessEqual),
            new OxlaBinaryOperator("<=", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.INT32, OxlaDataType.INT64}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyLessEqual),
            new OxlaBinaryOperator("<=", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.INT32, OxlaDataType.INT32}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyLessEqual),
            new OxlaBinaryOperator("<=", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.INTERVAL, OxlaDataType.INTERVAL}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyLessEqual),
            new OxlaBinaryOperator("<=", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.FLOAT32, OxlaDataType.FLOAT64}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyLessEqual),
            new OxlaBinaryOperator("<=", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.FLOAT32, OxlaDataType.FLOAT32}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyLessEqual),
            new OxlaBinaryOperator("<=", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.TEXT, OxlaDataType.TEXT}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyLessEqual),
            new OxlaBinaryOperator("<=", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.TIMESTAMP, OxlaDataType.DATE}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyLessEqual),
            new OxlaBinaryOperator("<=", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.TIMESTAMP, OxlaDataType.TIMESTAMP}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyLessEqual),
            new OxlaBinaryOperator("<=", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.TIMESTAMP, OxlaDataType.TIMESTAMPTZ}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyLessEqual),
            new OxlaBinaryOperator("<=", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.TIMESTAMPTZ, OxlaDataType.TIMESTAMP}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyLessEqual),
            new OxlaBinaryOperator("<=", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.TIMESTAMPTZ, OxlaDataType.TIMESTAMPTZ}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyLessEqual),
            new OxlaBinaryOperator("<=", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.TIME, OxlaDataType.TIME}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyLessEqual),
            // Not Equal
            new OxlaBinaryOperator("!=", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.INT64, OxlaDataType.INT64}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyNotEqual),
            new OxlaBinaryOperator("!=", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.INT64, OxlaDataType.INT32}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyNotEqual),
            new OxlaBinaryOperator("!=", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.BOOLEAN, OxlaDataType.BOOLEAN}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyNotEqual),
            new OxlaBinaryOperator("!=", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.DATE, OxlaDataType.DATE}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyNotEqual),
            new OxlaBinaryOperator("!=", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.DATE, OxlaDataType.TIMESTAMP}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyNotEqual),
            new OxlaBinaryOperator("!=", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.FLOAT64, OxlaDataType.FLOAT64}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyNotEqual),
            new OxlaBinaryOperator("!=", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.FLOAT64, OxlaDataType.FLOAT32}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyNotEqual),
            new OxlaBinaryOperator("!=", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.INT32, OxlaDataType.INT64}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyNotEqual),
            new OxlaBinaryOperator("!=", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.INT32, OxlaDataType.INT32}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyNotEqual),
            new OxlaBinaryOperator("!=", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.INTERVAL, OxlaDataType.INTERVAL}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyNotEqual),
            new OxlaBinaryOperator("!=", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.FLOAT32, OxlaDataType.FLOAT64}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyNotEqual),
            new OxlaBinaryOperator("!=", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.FLOAT32, OxlaDataType.FLOAT32}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyNotEqual),
            new OxlaBinaryOperator("!=", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.TEXT, OxlaDataType.TEXT}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyNotEqual),
            new OxlaBinaryOperator("!=", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.TIMESTAMP, OxlaDataType.DATE}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyNotEqual),
            new OxlaBinaryOperator("!=", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.TIMESTAMP, OxlaDataType.TIMESTAMP}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyNotEqual),
            new OxlaBinaryOperator("!=", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.TIMESTAMP, OxlaDataType.TIMESTAMPTZ}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyNotEqual),
            new OxlaBinaryOperator("!=", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.TIMESTAMPTZ, OxlaDataType.TIMESTAMP}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyNotEqual),
            new OxlaBinaryOperator("!=", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.TIMESTAMPTZ, OxlaDataType.TIMESTAMPTZ}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyNotEqual),
            new OxlaBinaryOperator("!=", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.TIME, OxlaDataType.TIME}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyNotEqual),
            // Equal
            new OxlaBinaryOperator("=", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.INT64, OxlaDataType.INT64}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyEqual),
            new OxlaBinaryOperator("=", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.INT64, OxlaDataType.INT32}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyEqual),
            new OxlaBinaryOperator("=", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.BOOLEAN, OxlaDataType.BOOLEAN}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyEqual),
            new OxlaBinaryOperator("=", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.DATE, OxlaDataType.DATE}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyEqual),
            new OxlaBinaryOperator("=", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.DATE, OxlaDataType.TIMESTAMP}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyEqual),
            new OxlaBinaryOperator("=", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.FLOAT64, OxlaDataType.FLOAT64}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyEqual),
            new OxlaBinaryOperator("=", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.FLOAT64, OxlaDataType.FLOAT32}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyEqual),
            new OxlaBinaryOperator("=", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.INT32, OxlaDataType.INT64}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyEqual),
            new OxlaBinaryOperator("=", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.INT32, OxlaDataType.INT32}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyEqual),
            new OxlaBinaryOperator("=", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.INTERVAL, OxlaDataType.INTERVAL}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyEqual),
            new OxlaBinaryOperator("=", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.FLOAT32, OxlaDataType.FLOAT64}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyEqual),
            new OxlaBinaryOperator("=", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.FLOAT32, OxlaDataType.FLOAT32}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyEqual),
            new OxlaBinaryOperator("=", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.TEXT, OxlaDataType.TEXT}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyEqual),
            new OxlaBinaryOperator("=", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.TIMESTAMP, OxlaDataType.DATE}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyEqual),
            new OxlaBinaryOperator("=", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.TIMESTAMP, OxlaDataType.TIMESTAMP}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyEqual),
            new OxlaBinaryOperator("=", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.TIMESTAMP, OxlaDataType.TIMESTAMPTZ}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyEqual),
            new OxlaBinaryOperator("=", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.TIMESTAMPTZ, OxlaDataType.TIMESTAMP}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyEqual),
            new OxlaBinaryOperator("=", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.TIMESTAMPTZ, OxlaDataType.TIMESTAMPTZ}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyEqual),
            new OxlaBinaryOperator("=", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.TIME, OxlaDataType.TIME}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyEqual),
            // Greater
            new OxlaBinaryOperator(">", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.INT64, OxlaDataType.INT64}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyGreater),
            new OxlaBinaryOperator(">", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.INT64, OxlaDataType.INT32}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyGreater),
            new OxlaBinaryOperator(">", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.BOOLEAN, OxlaDataType.BOOLEAN}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyGreater),
            new OxlaBinaryOperator(">", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.DATE, OxlaDataType.DATE}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyGreater),
            new OxlaBinaryOperator(">", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.DATE, OxlaDataType.TIMESTAMP}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyGreater),
            new OxlaBinaryOperator(">", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.FLOAT64, OxlaDataType.FLOAT64}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyGreater),
            new OxlaBinaryOperator(">", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.FLOAT64, OxlaDataType.FLOAT32}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyGreater),
            new OxlaBinaryOperator(">", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.INT32, OxlaDataType.INT64}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyGreater),
            new OxlaBinaryOperator(">", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.INT32, OxlaDataType.INT32}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyGreater),
            new OxlaBinaryOperator(">", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.INTERVAL, OxlaDataType.INTERVAL}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyGreater),
            new OxlaBinaryOperator(">", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.FLOAT32, OxlaDataType.FLOAT64}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyGreater),
            new OxlaBinaryOperator(">", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.FLOAT32, OxlaDataType.FLOAT32}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyGreater),
            new OxlaBinaryOperator(">", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.TEXT, OxlaDataType.TEXT}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyGreater),
            new OxlaBinaryOperator(">", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.TIMESTAMP, OxlaDataType.DATE}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyGreater),
            new OxlaBinaryOperator(">", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.TIMESTAMP, OxlaDataType.TIMESTAMP}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyGreater),
            new OxlaBinaryOperator(">", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.TIMESTAMP, OxlaDataType.TIMESTAMPTZ}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyGreater),
            new OxlaBinaryOperator(">", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.TIMESTAMPTZ, OxlaDataType.TIMESTAMP}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyGreater),
            new OxlaBinaryOperator(">", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.TIMESTAMPTZ, OxlaDataType.TIMESTAMPTZ}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyGreater),
            new OxlaBinaryOperator(">", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.TIME, OxlaDataType.TIME}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyGreater),
            // Greater Equal
            new OxlaBinaryOperator(">=", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.INT64, OxlaDataType.INT64}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyGreaterEqual),
            new OxlaBinaryOperator(">=", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.INT64, OxlaDataType.INT32}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyGreaterEqual),
            new OxlaBinaryOperator(">=", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.BOOLEAN, OxlaDataType.BOOLEAN}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyGreaterEqual),
            new OxlaBinaryOperator(">=", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.DATE, OxlaDataType.DATE}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyGreaterEqual),
            new OxlaBinaryOperator(">=", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.DATE, OxlaDataType.TIMESTAMP}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyGreaterEqual),
            new OxlaBinaryOperator(">=", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.FLOAT64, OxlaDataType.FLOAT64}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyGreaterEqual),
            new OxlaBinaryOperator(">=", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.FLOAT64, OxlaDataType.FLOAT32}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyGreaterEqual),
            new OxlaBinaryOperator(">=", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.INT32, OxlaDataType.INT64}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyGreaterEqual),
            new OxlaBinaryOperator(">=", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.INT32, OxlaDataType.INT32}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyGreaterEqual),
            new OxlaBinaryOperator(">=", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.INTERVAL, OxlaDataType.INTERVAL}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyGreaterEqual),
            new OxlaBinaryOperator(">=", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.FLOAT32, OxlaDataType.FLOAT64}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyGreaterEqual),
            new OxlaBinaryOperator(">=", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.FLOAT32, OxlaDataType.FLOAT32}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyGreaterEqual),
            new OxlaBinaryOperator(">=", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.TEXT, OxlaDataType.TEXT}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyGreaterEqual),
            new OxlaBinaryOperator(">=", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.TIMESTAMP, OxlaDataType.DATE}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyGreaterEqual),
            new OxlaBinaryOperator(">=", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.TIMESTAMP, OxlaDataType.TIMESTAMP}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyGreaterEqual),
            new OxlaBinaryOperator(">=", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.TIMESTAMP, OxlaDataType.TIMESTAMPTZ}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyGreaterEqual),
            new OxlaBinaryOperator(">=", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.TIMESTAMPTZ, OxlaDataType.TIMESTAMP}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyGreaterEqual),
            new OxlaBinaryOperator(">=", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.TIMESTAMPTZ, OxlaDataType.TIMESTAMPTZ}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyGreaterEqual),
            new OxlaBinaryOperator(">=", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.TIME, OxlaDataType.TIME}, OxlaDataType.BOOLEAN), OxlaBinaryOperation::applyGreaterEqual)
    );

    public static final List<OxlaOperator> LOGIC = List.of(
            new OxlaBinaryOperator("AND", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.BOOLEAN, OxlaDataType.BOOLEAN}, OxlaDataType.BOOLEAN), null),
            new OxlaBinaryOperator("OR", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.BOOLEAN, OxlaDataType.BOOLEAN}, OxlaDataType.BOOLEAN), null)
    );

    public static final List<OxlaOperator> ARITHMETIC = List.of(
            // Addition
            new OxlaBinaryOperator("+", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.DATE, OxlaDataType.INT32}, OxlaDataType.DATE), OxlaBinaryOperation::applyAdd),
            new OxlaBinaryOperator("+", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.DATE, OxlaDataType.INTERVAL}, OxlaDataType.TIMESTAMP), OxlaBinaryOperation::applyAdd),
            new OxlaBinaryOperator("+", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.DATE, OxlaDataType.TIME}, OxlaDataType.TIMESTAMP), OxlaBinaryOperation::applyAdd),
            new OxlaBinaryOperator("+", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.FLOAT32, OxlaDataType.FLOAT32}, OxlaDataType.FLOAT32), OxlaBinaryOperation::applyAdd),
            new OxlaBinaryOperator("+", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.FLOAT32, OxlaDataType.FLOAT64}, OxlaDataType.FLOAT64), OxlaBinaryOperation::applyAdd),
            new OxlaBinaryOperator("+", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.FLOAT64, OxlaDataType.FLOAT32}, OxlaDataType.FLOAT64), OxlaBinaryOperation::applyAdd),
            new OxlaBinaryOperator("+", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.FLOAT64, OxlaDataType.FLOAT64}, OxlaDataType.FLOAT64), OxlaBinaryOperation::applyAdd),
            new OxlaBinaryOperator("+", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.INT32, OxlaDataType.DATE}, OxlaDataType.DATE), OxlaBinaryOperation::applyAdd),
            new OxlaBinaryOperator("+", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.INT32, OxlaDataType.INT32}, OxlaDataType.INT32), OxlaBinaryOperation::applyAdd),
            new OxlaBinaryOperator("+", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.INT32, OxlaDataType.INT64}, OxlaDataType.INT64), OxlaBinaryOperation::applyAdd),
            new OxlaBinaryOperator("+", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.INT64, OxlaDataType.INT32}, OxlaDataType.INT64), OxlaBinaryOperation::applyAdd),
            new OxlaBinaryOperator("+", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.INT64, OxlaDataType.INT64}, OxlaDataType.INT64), OxlaBinaryOperation::applyAdd),
            new OxlaBinaryOperator("+", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.INTERVAL, OxlaDataType.DATE}, OxlaDataType.TIMESTAMP), OxlaBinaryOperation::applyAdd),
            new OxlaBinaryOperator("+", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.INTERVAL, OxlaDataType.INTERVAL}, OxlaDataType.INTERVAL), OxlaBinaryOperation::applyAdd),
            new OxlaBinaryOperator("+", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.INTERVAL, OxlaDataType.TIMESTAMPTZ}, OxlaDataType.TIMESTAMPTZ), OxlaBinaryOperation::applyAdd),
            new OxlaBinaryOperator("+", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.INTERVAL, OxlaDataType.TIMESTAMP}, OxlaDataType.TIMESTAMP), OxlaBinaryOperation::applyAdd),
            new OxlaBinaryOperator("+", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.INTERVAL, OxlaDataType.TIME}, OxlaDataType.TIME), OxlaBinaryOperation::applyAdd),
            new OxlaBinaryOperator("+", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.TIME, OxlaDataType.DATE}, OxlaDataType.TIMESTAMP), OxlaBinaryOperation::applyAdd),
            new OxlaBinaryOperator("+", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.TIME, OxlaDataType.INTERVAL}, OxlaDataType.TIME), OxlaBinaryOperation::applyAdd),
            new OxlaBinaryOperator("+", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.TIMESTAMP, OxlaDataType.INTERVAL}, OxlaDataType.TIMESTAMP), OxlaBinaryOperation::applyAdd),
            new OxlaBinaryOperator("+", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.TIMESTAMPTZ, OxlaDataType.INTERVAL}, OxlaDataType.TIMESTAMPTZ), OxlaBinaryOperation::applyAdd),
            // Subtraction
            new OxlaBinaryOperator("-", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.DATE, OxlaDataType.DATE}, OxlaDataType.INT32), OxlaBinaryOperation::applySub),
            new OxlaBinaryOperator("-", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.DATE, OxlaDataType.INT32}, OxlaDataType.DATE), OxlaBinaryOperation::applySub),
            new OxlaBinaryOperator("-", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.DATE, OxlaDataType.INTERVAL}, OxlaDataType.TIMESTAMP), OxlaBinaryOperation::applySub),
            new OxlaBinaryOperator("-", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.FLOAT32, OxlaDataType.FLOAT32}, OxlaDataType.FLOAT32), OxlaBinaryOperation::applySub),
            new OxlaBinaryOperator("-", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.FLOAT32, OxlaDataType.FLOAT64}, OxlaDataType.FLOAT64), OxlaBinaryOperation::applySub),
            new OxlaBinaryOperator("-", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.FLOAT64, OxlaDataType.FLOAT32}, OxlaDataType.FLOAT64), OxlaBinaryOperation::applySub),
            new OxlaBinaryOperator("-", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.FLOAT64, OxlaDataType.FLOAT64}, OxlaDataType.FLOAT64), OxlaBinaryOperation::applySub),
            new OxlaBinaryOperator("-", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.INT32, OxlaDataType.INT32}, OxlaDataType.INT32), OxlaBinaryOperation::applySub),
            new OxlaBinaryOperator("-", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.INT32, OxlaDataType.INT64}, OxlaDataType.INT64), OxlaBinaryOperation::applySub),
            new OxlaBinaryOperator("-", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.INT64, OxlaDataType.INT32}, OxlaDataType.INT64), OxlaBinaryOperation::applySub),
            new OxlaBinaryOperator("-", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.INT64, OxlaDataType.INT64}, OxlaDataType.INT64), OxlaBinaryOperation::applySub),
            new OxlaBinaryOperator("-", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.INTERVAL, OxlaDataType.INTERVAL}, OxlaDataType.INTERVAL), OxlaBinaryOperation::applySub),
            new OxlaBinaryOperator("-", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.TIME, OxlaDataType.INTERVAL}, OxlaDataType.TIME), OxlaBinaryOperation::applySub),
            new OxlaBinaryOperator("-", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.TIME, OxlaDataType.TIME}, OxlaDataType.INTERVAL), OxlaBinaryOperation::applySub),
            new OxlaBinaryOperator("-", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.TIMESTAMP, OxlaDataType.INTERVAL}, OxlaDataType.TIMESTAMP), OxlaBinaryOperation::applySub),
            new OxlaBinaryOperator("-", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.TIMESTAMP, OxlaDataType.TIMESTAMP}, OxlaDataType.INTERVAL), OxlaBinaryOperation::applySub),
            new OxlaBinaryOperator("-", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.TIMESTAMPTZ, OxlaDataType.INTERVAL}, OxlaDataType.TIMESTAMPTZ), OxlaBinaryOperation::applySub),
            new OxlaBinaryOperator("-", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.TIMESTAMPTZ, OxlaDataType.TIMESTAMPTZ}, OxlaDataType.INTERVAL), OxlaBinaryOperation::applySub),
            // Multiplication
            new OxlaBinaryOperator("*", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.FLOAT32, OxlaDataType.FLOAT32}, OxlaDataType.FLOAT32), OxlaBinaryOperation::applyMul),
            new OxlaBinaryOperator("*", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.FLOAT32, OxlaDataType.FLOAT64}, OxlaDataType.FLOAT64), OxlaBinaryOperation::applyMul),
            new OxlaBinaryOperator("*", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.FLOAT64, OxlaDataType.FLOAT32}, OxlaDataType.FLOAT64), OxlaBinaryOperation::applyMul),
            new OxlaBinaryOperator("*", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.FLOAT64, OxlaDataType.FLOAT64}, OxlaDataType.FLOAT64), OxlaBinaryOperation::applyMul),
            new OxlaBinaryOperator("*", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.FLOAT64, OxlaDataType.INTERVAL}, OxlaDataType.INTERVAL), OxlaBinaryOperation::applyMul),
            new OxlaBinaryOperator("*", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.INT32, OxlaDataType.INT32}, OxlaDataType.INT32), OxlaBinaryOperation::applyMul),
            new OxlaBinaryOperator("*", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.INT32, OxlaDataType.INT64}, OxlaDataType.INT64), OxlaBinaryOperation::applyMul),
            new OxlaBinaryOperator("*", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.INT32, OxlaDataType.INTERVAL}, OxlaDataType.INTERVAL), OxlaBinaryOperation::applyMul),
            new OxlaBinaryOperator("*", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.INT64, OxlaDataType.INT32}, OxlaDataType.INT64), OxlaBinaryOperation::applyMul),
            new OxlaBinaryOperator("*", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.INT64, OxlaDataType.INT64}, OxlaDataType.INT64), OxlaBinaryOperation::applyMul),
            new OxlaBinaryOperator("*", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.INT64, OxlaDataType.INTERVAL}, OxlaDataType.INTERVAL), OxlaBinaryOperation::applyMul),
            new OxlaBinaryOperator("*", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.INTERVAL, OxlaDataType.FLOAT32}, OxlaDataType.INTERVAL), OxlaBinaryOperation::applyMul),
            new OxlaBinaryOperator("*", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.INTERVAL, OxlaDataType.FLOAT64}, OxlaDataType.INTERVAL), OxlaBinaryOperation::applyMul),
            new OxlaBinaryOperator("*", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.INTERVAL, OxlaDataType.INT32}, OxlaDataType.INTERVAL), OxlaBinaryOperation::applyMul),
            new OxlaBinaryOperator("*", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.INTERVAL, OxlaDataType.INT64}, OxlaDataType.INTERVAL), OxlaBinaryOperation::applyMul),
            // Division
            new OxlaBinaryOperator("/", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.FLOAT32, OxlaDataType.FLOAT32}, OxlaDataType.FLOAT32), OxlaBinaryOperation::applyDiv),
            new OxlaBinaryOperator("/", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.FLOAT32, OxlaDataType.FLOAT64}, OxlaDataType.FLOAT64), OxlaBinaryOperation::applyDiv),
            new OxlaBinaryOperator("/", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.FLOAT64, OxlaDataType.FLOAT32}, OxlaDataType.FLOAT64), OxlaBinaryOperation::applyDiv),
            new OxlaBinaryOperator("/", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.FLOAT64, OxlaDataType.FLOAT64}, OxlaDataType.FLOAT64), OxlaBinaryOperation::applyDiv),
            new OxlaBinaryOperator("/", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.INT32, OxlaDataType.INT32}, OxlaDataType.INT32), OxlaBinaryOperation::applyDiv),
            new OxlaBinaryOperator("/", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.INT32, OxlaDataType.INT64}, OxlaDataType.INT64), OxlaBinaryOperation::applyDiv),
            new OxlaBinaryOperator("/", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.INT64, OxlaDataType.INT32}, OxlaDataType.INT64), OxlaBinaryOperation::applyDiv),
            new OxlaBinaryOperator("/", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.INT64, OxlaDataType.INT64}, OxlaDataType.INT64), OxlaBinaryOperation::applyDiv),
            new OxlaBinaryOperator("/", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.INTERVAL, OxlaDataType.FLOAT32}, OxlaDataType.INTERVAL), OxlaBinaryOperation::applyDiv),
            new OxlaBinaryOperator("/", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.INTERVAL, OxlaDataType.FLOAT64}, OxlaDataType.INTERVAL), OxlaBinaryOperation::applyDiv),
            new OxlaBinaryOperator("/", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.INTERVAL, OxlaDataType.INT32}, OxlaDataType.INTERVAL), OxlaBinaryOperation::applyDiv),
            new OxlaBinaryOperator("/", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.INTERVAL, OxlaDataType.INT64}, OxlaDataType.INTERVAL), OxlaBinaryOperation::applyDiv),
            // Modulus
            new OxlaBinaryOperator("%", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.FLOAT32, OxlaDataType.FLOAT32}, OxlaDataType.FLOAT32), OxlaBinaryOperation::applyMod),
            new OxlaBinaryOperator("%", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.FLOAT64, OxlaDataType.FLOAT64}, OxlaDataType.FLOAT64), OxlaBinaryOperation::applyMod),
            new OxlaBinaryOperator("%", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.INT32, OxlaDataType.INT32}, OxlaDataType.INT32), OxlaBinaryOperation::applyMod),
            new OxlaBinaryOperator("%", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.INT64, OxlaDataType.INT64}, OxlaDataType.INT64), OxlaBinaryOperation::applyMod)
    );

    public static final List<OxlaOperator> REGEX = List.of(
            new OxlaBinaryOperator("!~", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.TEXT, OxlaDataType.TEXT}, OxlaDataType.BOOLEAN), null),
            new OxlaBinaryOperator("!~*", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.TEXT, OxlaDataType.TEXT}, OxlaDataType.BOOLEAN), null),
            new OxlaBinaryOperator("!~~", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.TEXT, OxlaDataType.TEXT}, OxlaDataType.BOOLEAN), null),
            new OxlaBinaryOperator("!~~*", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.TEXT, OxlaDataType.TEXT}, OxlaDataType.BOOLEAN), null),
            new OxlaBinaryOperator("~", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.TEXT, OxlaDataType.TEXT}, OxlaDataType.BOOLEAN), null),
            new OxlaBinaryOperator("~*", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.TEXT, OxlaDataType.TEXT}, OxlaDataType.BOOLEAN), null),
            new OxlaBinaryOperator("~~", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.TEXT, OxlaDataType.TEXT}, OxlaDataType.BOOLEAN), null),
            new OxlaBinaryOperator("~~*", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.TEXT, OxlaDataType.TEXT}, OxlaDataType.BOOLEAN), null)

            // FIXME: These overloads take 3 input params - possibly move them to OxlaTernaryNode.
//            new OxlaBinaryOperator("!~~", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.TEXT, OxlaDataType.TEXT, OxlaDataType.TEXT}, OxlaDataType.BOOLEAN), null),
//            new OxlaBinaryOperator("!~~*", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.TEXT, OxlaDataType.TEXT, OxlaDataType.TEXT}, OxlaDataType.BOOLEAN), null),
//            new OxlaBinaryOperator("~~", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.TEXT, OxlaDataType.TEXT, OxlaDataType.TEXT}, OxlaDataType.BOOLEAN), null),
//            new OxlaBinaryOperator("~~*", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.TEXT, OxlaDataType.TEXT, OxlaDataType.TEXT}, OxlaDataType.BOOLEAN), null)
    );

    public static final List<OxlaOperator> BINARY = List.of(
            new OxlaBinaryOperator("#", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.INT32, OxlaDataType.INT32}, OxlaDataType.INT32), OxlaBinaryOperation::applyBitXor),
            new OxlaBinaryOperator("#", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.INT64, OxlaDataType.INT64}, OxlaDataType.INT64), OxlaBinaryOperation::applyBitXor),
            new OxlaBinaryOperator("&", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.INT32, OxlaDataType.INT32}, OxlaDataType.INT32), OxlaBinaryOperation::applyBitAnd),
            new OxlaBinaryOperator("&", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.INT64, OxlaDataType.INT64}, OxlaDataType.INT64), OxlaBinaryOperation::applyBitAnd),
            new OxlaBinaryOperator("^", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.FLOAT32, OxlaDataType.FLOAT32}, OxlaDataType.FLOAT32), OxlaBinaryOperation::applyBitPower),
            new OxlaBinaryOperator("^", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.FLOAT64, OxlaDataType.FLOAT64}, OxlaDataType.FLOAT64), OxlaBinaryOperation::applyBitPower),
            new OxlaBinaryOperator("^", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.INT32, OxlaDataType.INT32}, OxlaDataType.INT32), OxlaBinaryOperation::applyBitPower),
            new OxlaBinaryOperator("^", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.INT64, OxlaDataType.INT64}, OxlaDataType.INT64), OxlaBinaryOperation::applyBitPower),
            new OxlaBinaryOperator("|", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.INT32, OxlaDataType.INT32}, OxlaDataType.INT32), OxlaBinaryOperation::applyBitOr),
            new OxlaBinaryOperator("|", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.INT64, OxlaDataType.INT64}, OxlaDataType.INT64), OxlaBinaryOperation::applyBitOr)
    );

    public static final List<OxlaOperator> MISC = List.of(
            new OxlaBinaryOperator("->", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.JSON, OxlaDataType.INT32}, OxlaDataType.JSON), null),
            new OxlaBinaryOperator("->", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.JSON, OxlaDataType.TEXT}, OxlaDataType.JSON), null),
            new OxlaBinaryOperator("->>", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.JSON, OxlaDataType.INT32}, OxlaDataType.TEXT), null),
            new OxlaBinaryOperator("->>", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.JSON, OxlaDataType.TEXT}, OxlaDataType.TEXT), null),
            new OxlaBinaryOperator("AT TIME ZONE", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.TIMESTAMP, OxlaDataType.TEXT}, OxlaDataType.TIMESTAMPTZ), null),
            new OxlaBinaryOperator("AT TIME ZONE", new OxlaTypeOverload(new OxlaDataType[]{OxlaDataType.TIMESTAMPTZ, OxlaDataType.TEXT}, OxlaDataType.TIMESTAMP), null)
    );

    private static OxlaConstant applyLess(OxlaConstant[] constants) {
        return OxlaConstant.createBooleanConstant(constants[0].compareTo(constants[1]) < 0);
    }

    private static OxlaConstant applyLessEqual(OxlaConstant[] constants) {
        return OxlaConstant.createBooleanConstant(constants[0].compareTo(constants[1]) <= 0);
    }

    private static OxlaConstant applyNotEqual(OxlaConstant[] constants) {
        return OxlaConstant.createBooleanConstant(constants[0].compareTo(constants[1]) != 0);
    }

    private static OxlaConstant applyEqual(OxlaConstant[] constants) {
        return OxlaConstant.createBooleanConstant(constants[0].compareTo(constants[1]) == 0);
    }

    private static OxlaConstant applyGreater(OxlaConstant[] constants) {
        return OxlaConstant.createBooleanConstant(constants[0].compareTo(constants[1]) > 0);
    }

    private static OxlaConstant applyGreaterEqual(OxlaConstant[] constants) {
        return OxlaConstant.createBooleanConstant(constants[0].compareTo(constants[1]) >= 0);
    }

    private static OxlaConstant applyAdd(OxlaConstant[] constants) {
        final OxlaConstant left = constants[0];
        final OxlaConstant right = constants[1];
        if (left instanceof OxlaConstant.OxlaInt32Constant && right instanceof OxlaConstant.OxlaInt32Constant) {
            return OxlaConstant.createInt32Constant(((OxlaConstant.OxlaInt32Constant) left).value + ((OxlaConstant.OxlaInt32Constant) right).value);
        } else if (left instanceof OxlaConstant.OxlaInt64Constant && right instanceof OxlaConstant.OxlaInt64Constant) {
            return OxlaConstant.createInt64Constant(((OxlaConstant.OxlaInt64Constant) left).value + ((OxlaConstant.OxlaInt64Constant) right).value);
        } else if (left instanceof OxlaConstant.OxlaFloat32Constant && right instanceof OxlaConstant.OxlaFloat32Constant) {
            return OxlaConstant.createFloat32Constant(((OxlaConstant.OxlaFloat32Constant) left).value + ((OxlaConstant.OxlaFloat32Constant) right).value);
        } else if (left instanceof OxlaConstant.OxlaFloat64Constant && right instanceof OxlaConstant.OxlaFloat64Constant) {
            return OxlaConstant.createFloat64Constant(((OxlaConstant.OxlaFloat64Constant) left).value + ((OxlaConstant.OxlaFloat64Constant) right).value);
        }
        throw new IgnoreMeException(); // Not implemented for type.
    }

    private static OxlaConstant applySub(OxlaConstant[] constants) {
        final OxlaConstant left = constants[0];
        final OxlaConstant right = constants[1];
        if (left instanceof OxlaConstant.OxlaInt32Constant && right instanceof OxlaConstant.OxlaInt32Constant) {
            return OxlaConstant.createInt32Constant(((OxlaConstant.OxlaInt32Constant) left).value - ((OxlaConstant.OxlaInt32Constant) right).value);
        } else if (left instanceof OxlaConstant.OxlaInt64Constant && right instanceof OxlaConstant.OxlaInt64Constant) {
            return OxlaConstant.createInt64Constant(((OxlaConstant.OxlaInt64Constant) left).value - ((OxlaConstant.OxlaInt64Constant) right).value);
        } else if (left instanceof OxlaConstant.OxlaFloat32Constant && right instanceof OxlaConstant.OxlaFloat32Constant) {
            return OxlaConstant.createFloat32Constant(((OxlaConstant.OxlaFloat32Constant) left).value - ((OxlaConstant.OxlaFloat32Constant) right).value);
        } else if (left instanceof OxlaConstant.OxlaFloat64Constant && right instanceof OxlaConstant.OxlaFloat64Constant) {
            return OxlaConstant.createFloat64Constant(((OxlaConstant.OxlaFloat64Constant) left).value - ((OxlaConstant.OxlaFloat64Constant) right).value);
        }
        throw new IgnoreMeException(); // Not implemented for type.
    }

    private static OxlaConstant applyMul(OxlaConstant[] constants) {
        final OxlaConstant left = constants[0];
        final OxlaConstant right = constants[1];
        if (left instanceof OxlaConstant.OxlaInt32Constant && right instanceof OxlaConstant.OxlaInt32Constant) {
            return OxlaConstant.createInt32Constant(((OxlaConstant.OxlaInt32Constant) left).value * ((OxlaConstant.OxlaInt32Constant) right).value);
        } else if (left instanceof OxlaConstant.OxlaInt64Constant && right instanceof OxlaConstant.OxlaInt64Constant) {
            return OxlaConstant.createInt64Constant(((OxlaConstant.OxlaInt64Constant) left).value * ((OxlaConstant.OxlaInt64Constant) right).value);
        } else if (left instanceof OxlaConstant.OxlaFloat32Constant && right instanceof OxlaConstant.OxlaFloat32Constant) {
            return OxlaConstant.createFloat32Constant(((OxlaConstant.OxlaFloat32Constant) left).value * ((OxlaConstant.OxlaFloat32Constant) right).value);
        } else if (left instanceof OxlaConstant.OxlaFloat64Constant && right instanceof OxlaConstant.OxlaFloat64Constant) {
            return OxlaConstant.createFloat64Constant(((OxlaConstant.OxlaFloat64Constant) left).value * ((OxlaConstant.OxlaFloat64Constant) right).value);
        }
        throw new IgnoreMeException(); // Not implemented for type.
    }

    private static OxlaConstant applyDiv(OxlaConstant[] constants) {
        final OxlaConstant left = constants[0];
        final OxlaConstant right = constants[1];
        if (left instanceof OxlaConstant.OxlaInt32Constant && right instanceof OxlaConstant.OxlaInt32Constant) {
            return OxlaConstant.createInt32Constant(((OxlaConstant.OxlaInt32Constant) left).value / ((OxlaConstant.OxlaInt32Constant) right).value);
        } else if (left instanceof OxlaConstant.OxlaInt64Constant && right instanceof OxlaConstant.OxlaInt64Constant) {
            return OxlaConstant.createInt64Constant(((OxlaConstant.OxlaInt64Constant) left).value / ((OxlaConstant.OxlaInt64Constant) right).value);
        } else if (left instanceof OxlaConstant.OxlaFloat32Constant && right instanceof OxlaConstant.OxlaFloat32Constant) {
            return OxlaConstant.createFloat32Constant(((OxlaConstant.OxlaFloat32Constant) left).value / ((OxlaConstant.OxlaFloat32Constant) right).value);
        } else if (left instanceof OxlaConstant.OxlaFloat64Constant && right instanceof OxlaConstant.OxlaFloat64Constant) {
            return OxlaConstant.createFloat64Constant(((OxlaConstant.OxlaFloat64Constant) left).value / ((OxlaConstant.OxlaFloat64Constant) right).value);
        }
        throw new IgnoreMeException(); // Not implemented for type.
    }

    private static OxlaConstant applyMod(OxlaConstant[] constants) {
        OxlaConstant left = constants[0];
        OxlaConstant right = constants[1];
        if (left instanceof OxlaConstant.OxlaInt32Constant && right instanceof OxlaConstant.OxlaInt32Constant) {
            return OxlaConstant.createInt32Constant(((OxlaConstant.OxlaInt32Constant) left).value % ((OxlaConstant.OxlaInt32Constant) right).value);
        } else if (left instanceof OxlaConstant.OxlaInt64Constant && right instanceof OxlaConstant.OxlaInt64Constant) {
            return OxlaConstant.createInt64Constant(((OxlaConstant.OxlaInt64Constant) left).value % ((OxlaConstant.OxlaInt64Constant) right).value);
        } else if (left instanceof OxlaConstant.OxlaFloat32Constant && right instanceof OxlaConstant.OxlaFloat32Constant) {
            return OxlaConstant.createFloat32Constant(((OxlaConstant.OxlaFloat32Constant) left).value % ((OxlaConstant.OxlaFloat32Constant) right).value);
        } else if (left instanceof OxlaConstant.OxlaFloat64Constant && right instanceof OxlaConstant.OxlaFloat64Constant) {
            return OxlaConstant.createFloat64Constant(((OxlaConstant.OxlaFloat64Constant) left).value % ((OxlaConstant.OxlaFloat64Constant) right).value);
        }
        throw new AssertionError(String.format("OxlaBinaryOperationOperation::applyMod failed: %s vs %s", left.getClass(), right.getClass()));
    }

    private static OxlaConstant applyBitXor(OxlaConstant[] constants) {
        final OxlaConstant left = constants[0];
        final OxlaConstant right = constants[1];
        if (left instanceof OxlaConstant.OxlaInt32Constant && right instanceof OxlaConstant.OxlaInt32Constant) {
            return OxlaConstant.createInt32Constant(((OxlaConstant.OxlaInt32Constant) left).value ^ ((OxlaConstant.OxlaInt32Constant) right).value);
        } else if (left instanceof OxlaConstant.OxlaInt64Constant && right instanceof OxlaConstant.OxlaInt64Constant) {
            return OxlaConstant.createInt64Constant(((OxlaConstant.OxlaInt64Constant) left).value ^ ((OxlaConstant.OxlaInt64Constant) right).value);
        }
        throw new AssertionError(String.format("OxlaBinaryOperationOperation::applyBitXor failed: %s vs %s", constants[0].getClass(), constants[1].getClass()));
    }

    private static OxlaConstant applyBitAnd(OxlaConstant[] constants) {
        final OxlaConstant left = constants[0];
        final OxlaConstant right = constants[1];
        if (left instanceof OxlaConstant.OxlaInt32Constant && right instanceof OxlaConstant.OxlaInt32Constant) {
            return OxlaConstant.createInt32Constant(((OxlaConstant.OxlaInt32Constant) left).value & ((OxlaConstant.OxlaInt32Constant) right).value);
        } else if (left instanceof OxlaConstant.OxlaInt64Constant && right instanceof OxlaConstant.OxlaInt64Constant) {
            return OxlaConstant.createInt64Constant(((OxlaConstant.OxlaInt64Constant) left).value & ((OxlaConstant.OxlaInt64Constant) right).value);
        }
        throw new AssertionError(String.format("OxlaBinaryOperationOperation::applyBitAnd failed: %s vs %s", constants[0].getClass(), constants[1].getClass()));
    }

    private static OxlaConstant applyBitPower(OxlaConstant[] constants) {
        final OxlaConstant left = constants[0];
        final OxlaConstant right = constants[1];
        if (left instanceof OxlaConstant.OxlaInt32Constant && right instanceof OxlaConstant.OxlaInt32Constant) {
            return OxlaConstant.createInt32Constant((int) Math.pow(((OxlaConstant.OxlaInt32Constant) left).value, ((OxlaConstant.OxlaInt32Constant) right).value));
        } else if (left instanceof OxlaConstant.OxlaInt64Constant && right instanceof OxlaConstant.OxlaInt64Constant) {
            return OxlaConstant.createInt64Constant((long) Math.pow(((OxlaConstant.OxlaInt64Constant) left).value, ((OxlaConstant.OxlaInt64Constant) right).value));
        } else if (left instanceof OxlaConstant.OxlaFloat32Constant && right instanceof OxlaConstant.OxlaFloat32Constant) {
            return OxlaConstant.createFloat32Constant((float) Math.pow(((OxlaConstant.OxlaFloat32Constant) left).value, ((OxlaConstant.OxlaFloat32Constant) right).value));
        } else if (left instanceof OxlaConstant.OxlaFloat64Constant && right instanceof OxlaConstant.OxlaFloat64Constant) {
            return OxlaConstant.createFloat64Constant(Math.pow(((OxlaConstant.OxlaFloat64Constant) left).value, ((OxlaConstant.OxlaFloat64Constant) right).value));
        }
        throw new AssertionError(String.format("OxlaBinaryOperationOperation::applyBitPower failed: %s vs %s", constants[0].getClass(), constants[1].getClass()));
    }

    private static OxlaConstant applyBitOr(OxlaConstant[] constants) {
        final OxlaConstant left = constants[0];
        final OxlaConstant right = constants[1];
        if (left instanceof OxlaConstant.OxlaInt32Constant && right instanceof OxlaConstant.OxlaInt32Constant) {
            return OxlaConstant.createInt32Constant(((OxlaConstant.OxlaInt32Constant) left).value | ((OxlaConstant.OxlaInt32Constant) right).value);
        } else if (left instanceof OxlaConstant.OxlaInt64Constant && right instanceof OxlaConstant.OxlaInt64Constant) {
            return OxlaConstant.createInt64Constant(((OxlaConstant.OxlaInt64Constant) left).value | ((OxlaConstant.OxlaInt64Constant) right).value);
        }
        throw new AssertionError(String.format("OxlaBinaryOperationOperation::applyBitOr failed: %s vs %s", constants[0].getClass(), constants[1].getClass()));
    }
}
