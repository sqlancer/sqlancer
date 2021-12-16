package sqlancer.oceanbase.ast;

import sqlancer.Randomly;
import sqlancer.oceanbase.OceanBaseSchema.OceanBaseDataType;
import sqlancer.oceanbase.ast.OceanBaseUnaryPrefixOperation.OceanBaseUnaryPrefixOperator;

public class OceanBaseBinaryComparisonOperation implements OceanBaseExpression {

    public enum BinaryComparisonOperator {
        EQUALS("=") {
            @Override
            public OceanBaseConstant getExpectedValue(OceanBaseConstant leftVal, OceanBaseConstant rightVal) {
                return leftVal.isEquals(rightVal);
            }
        },
        NOT_EQUALS("!=") {
            @Override
            public OceanBaseConstant getExpectedValue(OceanBaseConstant leftVal, OceanBaseConstant rightVal) {
                OceanBaseConstant isEquals = leftVal.isEquals(rightVal);
                if (isEquals.getType() == OceanBaseDataType.INT) {
                    return OceanBaseConstant.createIntConstant(1 - isEquals.getInt());
                }
                return isEquals;
            }
        },
        LESS("<") {
            @Override
            public OceanBaseConstant getExpectedValue(OceanBaseConstant leftVal, OceanBaseConstant rightVal) {
                return leftVal.isLessThan(rightVal);
            }
        },
        LESS_EQUALS("<=") {

            @Override
            public OceanBaseConstant getExpectedValue(OceanBaseConstant leftVal, OceanBaseConstant rightVal) {
                OceanBaseConstant lessThan = leftVal.isLessThan(rightVal);
                if (lessThan == null) {
                    return null;
                }
                if (lessThan.getType() == OceanBaseDataType.INT && lessThan.getInt() == 0) {
                    return leftVal.isEquals(rightVal);
                } else {
                    return lessThan;
                }
            }
        },
        GREATER(">") {
            @Override
            public OceanBaseConstant getExpectedValue(OceanBaseConstant leftVal, OceanBaseConstant rightVal) {
                OceanBaseConstant equals = leftVal.isEquals(rightVal);
                if (equals.getType() == OceanBaseDataType.INT && equals.getInt() == 1) {
                    return OceanBaseConstant.createFalse();
                } else {
                    OceanBaseConstant applyLess = leftVal.isLessThan(rightVal);
                    if (applyLess.isNull()) {
                        return OceanBaseConstant.createNullConstant();
                    }
                    return OceanBaseUnaryPrefixOperator.NOT.applyNotNull(applyLess);
                }
            }
        },
        GREATER_EQUALS(">=") {
            @Override
            public OceanBaseConstant getExpectedValue(OceanBaseConstant leftVal, OceanBaseConstant rightVal) {
                OceanBaseConstant equals = leftVal.isEquals(rightVal);
                if (equals.getType() == OceanBaseDataType.INT && equals.getInt() == 1) {
                    return OceanBaseConstant.createTrue();
                } else {
                    OceanBaseConstant applyLess = leftVal.isLessThan(rightVal);
                    if (applyLess.isNull()) {
                        return OceanBaseConstant.createNullConstant();
                    }
                    return OceanBaseUnaryPrefixOperator.NOT.applyNotNull(applyLess);
                }
            }
        };

        private final String textRepresentation;

        public String getTextRepresentation() {
            return textRepresentation;
        }

        BinaryComparisonOperator(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        public abstract OceanBaseConstant getExpectedValue(OceanBaseConstant leftVal, OceanBaseConstant rightVal);

        public static BinaryComparisonOperator getRandom() {
            return Randomly.fromOptions(BinaryComparisonOperator.values());
        }
    }

    private final OceanBaseExpression left;
    private final OceanBaseExpression right;
    private final BinaryComparisonOperator op;

    public OceanBaseBinaryComparisonOperation(OceanBaseExpression left, OceanBaseExpression right,
            BinaryComparisonOperator op) {
        this.left = left;
        this.right = right;
        this.op = op;
    }

    public OceanBaseExpression getLeft() {
        return left;
    }

    public BinaryComparisonOperator getOp() {
        return op;
    }

    public OceanBaseExpression getRight() {
        return right;
    }

    @Override
    public OceanBaseConstant getExpectedValue() {
        return op.getExpectedValue(left.getExpectedValue(), right.getExpectedValue());
    }

}
