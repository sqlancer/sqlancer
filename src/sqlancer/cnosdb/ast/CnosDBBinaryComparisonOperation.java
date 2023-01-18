package sqlancer.cnosdb.ast;

import sqlancer.Randomly;
import sqlancer.cnosdb.CnosDBSchema.CnosDBDataType;
import sqlancer.cnosdb.ast.CnosDBBinaryComparisonOperation.CnosDBBinaryComparisonOperator;
import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.BinaryOperatorNode.Operator;

public class CnosDBBinaryComparisonOperation
        extends BinaryOperatorNode<CnosDBExpression, CnosDBBinaryComparisonOperator> implements CnosDBExpression {

    public enum CnosDBBinaryComparisonOperator implements Operator {
        EQUALS("=") {
            @Override
            public CnosDBConstant getExpectedValue(CnosDBConstant leftVal, CnosDBConstant rightVal) {
                return leftVal.isEquals(rightVal);
            }
        },
        IS_DISTINCT("IS DISTINCT FROM") {
            @Override
            public CnosDBConstant getExpectedValue(CnosDBConstant leftVal, CnosDBConstant rightVal) {
                return CnosDBConstant
                        .createBooleanConstant(!IS_NOT_DISTINCT.getExpectedValue(leftVal, rightVal).asBoolean());
            }
        },
        IS_NOT_DISTINCT("IS NOT DISTINCT FROM") {
            @Override
            public CnosDBConstant getExpectedValue(CnosDBConstant leftVal, CnosDBConstant rightVal) {
                if (leftVal.isNull()) {
                    return CnosDBConstant.createBooleanConstant(rightVal.isNull());
                } else if (rightVal.isNull()) {
                    return CnosDBConstant.createFalse();
                } else {
                    return leftVal.isEquals(rightVal);
                }
            }
        },
        NOT_EQUALS("!=") {
            @Override
            public CnosDBConstant getExpectedValue(CnosDBConstant leftVal, CnosDBConstant rightVal) {
                CnosDBConstant isEquals = leftVal.isEquals(rightVal);
                if (isEquals.isBoolean()) {
                    return CnosDBConstant.createBooleanConstant(!isEquals.asBoolean());
                }
                return isEquals;
            }
        },
        LESS("<") {
            @Override
            public CnosDBConstant getExpectedValue(CnosDBConstant leftVal, CnosDBConstant rightVal) {
                return leftVal.isLessThan(rightVal);
            }
        },
        LESS_EQUALS("<=") {
            @Override
            public CnosDBConstant getExpectedValue(CnosDBConstant leftVal, CnosDBConstant rightVal) {
                CnosDBConstant lessThan = leftVal.isLessThan(rightVal);
                if (lessThan.isBoolean() && !lessThan.asBoolean()) {
                    return leftVal.isEquals(rightVal);
                } else {
                    return lessThan;
                }
            }
        },
        GREATER(">") {
            @Override
            public CnosDBConstant getExpectedValue(CnosDBConstant leftVal, CnosDBConstant rightVal) {
                CnosDBConstant equals = leftVal.isEquals(rightVal);
                if (equals.isBoolean() && equals.asBoolean()) {
                    return CnosDBConstant.createFalse();
                } else {
                    CnosDBConstant applyLess = leftVal.isLessThan(rightVal);
                    if (applyLess.isNull()) {
                        return CnosDBConstant.createNullConstant();
                    }
                    return CnosDBPrefixOperation.PrefixOperator.NOT.getExpectedValue(applyLess);
                }
            }
        },
        GREATER_EQUALS(">=") {
            @Override
            public CnosDBConstant getExpectedValue(CnosDBConstant leftVal, CnosDBConstant rightVal) {
                CnosDBConstant equals = leftVal.isEquals(rightVal);
                if (equals.isBoolean() && equals.asBoolean()) {
                    return CnosDBConstant.createTrue();
                } else {
                    CnosDBConstant applyLess = leftVal.isLessThan(rightVal);
                    if (applyLess.isNull()) {
                        return CnosDBConstant.createNullConstant();
                    }
                    return CnosDBPrefixOperation.PrefixOperator.NOT.getExpectedValue(applyLess);
                }
            }

        };

        private final String textRepresentation;

        @Override
        public String getTextRepresentation() {
            return textRepresentation;
        }

        CnosDBBinaryComparisonOperator(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        public abstract CnosDBConstant getExpectedValue(CnosDBConstant leftVal, CnosDBConstant rightVal);

        public static CnosDBBinaryComparisonOperator getRandom() {
            return Randomly.fromOptions(CnosDBBinaryComparisonOperator.values());
        }

    }

    public CnosDBBinaryComparisonOperation(CnosDBExpression left, CnosDBExpression right,
            CnosDBBinaryComparisonOperator op) {
        super(left, right, op);
    }

    @Override
    public CnosDBConstant getExpectedValue() {
        CnosDBConstant leftExpectedValue = getLeft().getExpectedValue();
        CnosDBConstant rightExpectedValue = getRight().getExpectedValue();
        if (leftExpectedValue == null || rightExpectedValue == null) {
            return null;
        }
        return getOp().getExpectedValue(leftExpectedValue, rightExpectedValue);
    }

    @Override
    public CnosDBDataType getExpressionType() {
        return CnosDBDataType.BOOLEAN;
    }

}
