package sqlancer.oxla.ast;

import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.newast.NewUnaryPrefixOperatorNode;
import sqlancer.oxla.schema.OxlaDataType;

import java.util.List;
import java.util.stream.Collectors;

public class OxlaUnaryPrefixOperation extends NewUnaryPrefixOperatorNode<OxlaExpression>
        implements OxlaExpression {
    public OxlaUnaryPrefixOperation(OxlaExpression expr, BinaryOperatorNode.Operator op) {
        super(expr, op);
    }

    @Override
    public OxlaConstant getExpectedValue() {
        OxlaConstant expectedValue = getExpr().getExpectedValue();
        if (expectedValue == null) {
            return null;
        }
        return ((OxlaUnaryPrefixOperator) op).apply(new OxlaConstant[]{expectedValue});
    }

    public static class OxlaUnaryPrefixOperator extends OxlaOperator {
        private final OxlaApplyFunction applyFunction;

        public OxlaUnaryPrefixOperator(String textRepresentation, OxlaTypeOverload overload, OxlaApplyFunction applyFunction) {
            super(textRepresentation, overload);
            this.applyFunction = applyFunction;
        }

        public OxlaConstant apply(OxlaConstant[] constants) {
            if (constants.length != 1) {
                throw new AssertionError(String.format("OxlaUnaryPrefixOperation::apply* failed: expected 1 argument, but got %d", constants.length));
            }
            if (constants[0] instanceof OxlaConstant.OxlaNullConstant) {
                return constants[0]; // No-op.
            }
            return applyFunction.apply(constants);
        }
    }

    // FIXME Imho, this class shouldn't hardcode the operators, but query the database for them instead.
    //       Sadly, Oxla does not support every pg_* table in the metastore so it's impossible for now.
    public static final List<OxlaOperator> ALL = List.of(
            new OxlaUnaryPrefixOperator("+", new OxlaTypeOverload(OxlaDataType.FLOAT32, OxlaDataType.FLOAT32), OxlaUnaryPrefixOperation::applyPlus),
            new OxlaUnaryPrefixOperator("+", new OxlaTypeOverload(OxlaDataType.FLOAT64, OxlaDataType.FLOAT64), OxlaUnaryPrefixOperation::applyPlus),
            new OxlaUnaryPrefixOperator("+", new OxlaTypeOverload(OxlaDataType.INT32, OxlaDataType.INT32), OxlaUnaryPrefixOperation::applyPlus),
            new OxlaUnaryPrefixOperator("+", new OxlaTypeOverload(OxlaDataType.INT64, OxlaDataType.INT64), OxlaUnaryPrefixOperation::applyPlus),
            new OxlaUnaryPrefixOperator("-", new OxlaTypeOverload(OxlaDataType.FLOAT32, OxlaDataType.FLOAT32), OxlaUnaryPrefixOperation::applyMinus),
            new OxlaUnaryPrefixOperator("-", new OxlaTypeOverload(OxlaDataType.FLOAT64, OxlaDataType.FLOAT64), OxlaUnaryPrefixOperation::applyMinus),
            new OxlaUnaryPrefixOperator("-", new OxlaTypeOverload(OxlaDataType.INT32, OxlaDataType.INT32), OxlaUnaryPrefixOperation::applyMinus),
            new OxlaUnaryPrefixOperator("-", new OxlaTypeOverload(OxlaDataType.INT64, OxlaDataType.INT64), OxlaUnaryPrefixOperation::applyMinus),
            new OxlaUnaryPrefixOperator("-", new OxlaTypeOverload(OxlaDataType.INTERVAL, OxlaDataType.INTERVAL), OxlaUnaryPrefixOperation::applyMinus),
            new OxlaUnaryPrefixOperator("@", new OxlaTypeOverload(OxlaDataType.FLOAT32, OxlaDataType.FLOAT32), OxlaUnaryPrefixOperation::applyAbs),
            new OxlaUnaryPrefixOperator("@", new OxlaTypeOverload(OxlaDataType.FLOAT64, OxlaDataType.FLOAT64), OxlaUnaryPrefixOperation::applyAbs),
            new OxlaUnaryPrefixOperator("@", new OxlaTypeOverload(OxlaDataType.INT32, OxlaDataType.INT32), OxlaUnaryPrefixOperation::applyAbs),
            new OxlaUnaryPrefixOperator("@", new OxlaTypeOverload(OxlaDataType.INT64, OxlaDataType.INT64), OxlaUnaryPrefixOperation::applyAbs),
            new OxlaUnaryPrefixOperator("NOT", new OxlaTypeOverload(OxlaDataType.BOOLEAN, OxlaDataType.BOOLEAN), OxlaUnaryPrefixOperation::applyNot),
            new OxlaUnaryPrefixOperator("|/", new OxlaTypeOverload(OxlaDataType.FLOAT32, OxlaDataType.FLOAT64), OxlaUnaryPrefixOperation::applySqrt),
            new OxlaUnaryPrefixOperator("|/", new OxlaTypeOverload(OxlaDataType.FLOAT64, OxlaDataType.FLOAT64), OxlaUnaryPrefixOperation::applySqrt),
            new OxlaUnaryPrefixOperator("||/", new OxlaTypeOverload(OxlaDataType.FLOAT32, OxlaDataType.FLOAT64), OxlaUnaryPrefixOperation::applyCbrt),
            new OxlaUnaryPrefixOperator("||/", new OxlaTypeOverload(OxlaDataType.FLOAT64, OxlaDataType.FLOAT64), OxlaUnaryPrefixOperation::applyCbrt),
            new OxlaUnaryPrefixOperator("~", new OxlaTypeOverload(OxlaDataType.INT32, OxlaDataType.INT32), OxlaUnaryPrefixOperation::applyBitNot),
            new OxlaUnaryPrefixOperator("~", new OxlaTypeOverload(OxlaDataType.INT64, OxlaDataType.INT64), OxlaUnaryPrefixOperation::applyBitNot)
//            new OxlaUnaryPrefixOperator("~", new OxlaTypeOverload(OxlaDataType.TEXT, OxlaDataType.TEXT)) // FIXME Generate only for REGEXes.
    );
    public static final OxlaOperator NOT = ALL.get(13);

    public static List<OxlaOperator> getForType(OxlaDataType returnType) {
        return ALL.stream()
                .filter(op -> (op.overload.returnType == returnType))
                .collect(Collectors.toList());
    }

    private static OxlaConstant applyPlus(OxlaConstant[] constants) {
        return constants[0]; // No-op.
    }

    private static OxlaConstant applyMinus(OxlaConstant[] constants) {
        final OxlaConstant constant = constants[0];
        if (constant instanceof OxlaConstant.OxlaIntervalConstant) {
            return OxlaConstant.createIntervalConstant(
                    -((OxlaConstant.OxlaIntervalConstant) constant).months,
                    -((OxlaConstant.OxlaIntervalConstant) constant).days,
                    -((OxlaConstant.OxlaIntervalConstant) constant).microseconds
            );
        } else if (constant instanceof OxlaConstant.OxlaFloat32Constant) {
            return OxlaConstant.createFloat32Constant(-((OxlaConstant.OxlaFloat32Constant) constant).value);
        } else if (constant instanceof OxlaConstant.OxlaFloat64Constant) {
            return OxlaConstant.createFloat64Constant(-((OxlaConstant.OxlaFloat64Constant) constant).value);
        } else if (constant instanceof OxlaConstant.OxlaIntegerConstant) {
            return OxlaConstant.createInt64Constant(-((OxlaConstant.OxlaIntegerConstant) constant).value);
        }
        throw new AssertionError(String.format("OxlaUnaryPrefixOperation::applyMinus failed: %s", constant.getClass()));
    }

    private static OxlaConstant applyAbs(OxlaConstant[] constants) {
        final OxlaConstant constant = constants[0];
        if (constant instanceof OxlaConstant.OxlaFloat32Constant) {
            return OxlaConstant.createFloat32Constant(Math.abs(((OxlaConstant.OxlaFloat32Constant) constant).value));
        } else if (constant instanceof OxlaConstant.OxlaFloat64Constant) {
            return OxlaConstant.createFloat64Constant(Math.abs(((OxlaConstant.OxlaFloat64Constant) constant).value));
        } else if (constant instanceof OxlaConstant.OxlaIntegerConstant) {
            return OxlaConstant.createInt64Constant(Math.abs(((OxlaConstant.OxlaIntegerConstant) constant).value));
        }
        throw new AssertionError(String.format("OxlaUnaryPrefixOperation::applyAbs failed: %s", constant.getClass()));
    }

    private static OxlaConstant applyNot(OxlaConstant[] constants) {
        final OxlaConstant constant = constants[0];
        if (constant instanceof OxlaConstant.OxlaBooleanConstant) {
            return OxlaConstant.createBooleanConstant(!((OxlaConstant.OxlaBooleanConstant) constant).value);
        }
        throw new AssertionError(String.format("OxlaUnaryPrefixOperation::applyNot failed: %s", constant.getClass()));
    }

    private static OxlaConstant applySqrt(OxlaConstant[] constants) {
        final OxlaConstant constant = constants[0];
        if (constant instanceof OxlaConstant.OxlaFloat32Constant) {
            return OxlaConstant.createFloat32Constant((float) Math.sqrt(((OxlaConstant.OxlaFloat32Constant) constant).value));
        } else if (constant instanceof OxlaConstant.OxlaFloat64Constant) {
            return OxlaConstant.createFloat64Constant(Math.sqrt(((OxlaConstant.OxlaFloat64Constant) constant).value));
        }
        throw new AssertionError(String.format("OxlaUnaryPrefixOperation::applySqrt failed: %s", constant.getClass()));
    }

    private static OxlaConstant applyCbrt(OxlaConstant[] constants) {
        final OxlaConstant constant = constants[0];
        if (constant instanceof OxlaConstant.OxlaFloat32Constant) {
            return OxlaConstant.createFloat32Constant((float) Math.cbrt(((OxlaConstant.OxlaFloat32Constant) constant).value));
        } else if (constant instanceof OxlaConstant.OxlaFloat64Constant) {
            return OxlaConstant.createFloat64Constant(Math.cbrt(((OxlaConstant.OxlaFloat64Constant) constant).value));
        }
        throw new AssertionError(String.format("OxlaUnaryPrefixOperation::applyCbrt failed: %s", constant.getClass()));
    }

    private static OxlaConstant applyBitNot(OxlaConstant[] constants) {
        final OxlaConstant constant = constants[0];
        if (constant instanceof OxlaConstant.OxlaIntegerConstant) {
            return OxlaConstant.createInt64Constant(~((OxlaConstant.OxlaIntegerConstant) constant).value);
        }
        throw new AssertionError(String.format("OxlaUnaryPrefixOperation::applyCbrt failed: %s", constant.getClass()));
    }
}
