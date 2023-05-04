package sqlancer.databend.ast;

import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.newast.NewBinaryOperatorNode;
import sqlancer.databend.DatabendExprToNode;
import sqlancer.databend.DatabendSchema.DatabendDataType;

public class DatabendBinaryComparisonOperation extends NewBinaryOperatorNode<DatabendExpression>
        implements DatabendExpression {

    public DatabendBinaryComparisonOperation(DatabendExpression left, DatabendExpression right,
            DatabendBinaryComparisonOperator op) {
        super(DatabendExprToNode.cast(left), DatabendExprToNode.cast(right), op);
    }

    public DatabendExpression getLeftExpression() {
        return (DatabendExpression) super.getLeft();
    }

    public DatabendExpression getRightExpression() {
        return (DatabendExpression) super.getRight();
    }

    public DatabendBinaryComparisonOperator getOp() {
        return (DatabendBinaryComparisonOperator) op;
    }

    @Override
    public DatabendDataType getExpectedType() {
        return DatabendDataType.BOOLEAN;
    }

    @Override
    public DatabendConstant getExpectedValue() {
        DatabendConstant leftExpectedValue = getLeftExpression().getExpectedValue();
        DatabendConstant rightExpectedValue = getRightExpression().getExpectedValue();
        if (leftExpectedValue == null || rightExpectedValue == null) {
            return null;
        }
        return getOp().apply(leftExpectedValue, rightExpectedValue);
    }

    public enum DatabendBinaryComparisonOperator implements BinaryOperatorNode.Operator {
        EQUALS("=") {
            @Override
            public DatabendConstant apply(DatabendConstant left, DatabendConstant right) {
                return left.isEquals(right);
            }
        },
        NOT_EQUALS("!=") {
            @Override
            public DatabendConstant apply(DatabendConstant left, DatabendConstant right) {
                DatabendConstant isEquals = left.isEquals(right);
                if (isEquals.isBoolean()) {
                    return DatabendConstant.createBooleanConstant(!isEquals.asBoolean());
                }
                return isEquals;
            }
        },
        IS_DISTINCT("IS DISTINCT FROM") {
            @Override
            public DatabendConstant apply(DatabendConstant left, DatabendConstant right) {
                return DatabendConstant.createBooleanConstant(!IS_NOT_DISTINCT.apply(left, right).asBoolean());
            }
        },
        IS_NOT_DISTINCT("IS NOT DISTINCT FROM") {
            @Override
            public DatabendConstant apply(DatabendConstant left, DatabendConstant right) {
                if (left.isNull()) {
                    return DatabendConstant.createBooleanConstant(right.isNull());
                } else if (right.isNull()) {
                    return DatabendConstant.createBooleanConstant(false);
                } else {
                    return left.isEquals(right);
                }
            }
        },
        LESS("<") {
            @Override
            public DatabendConstant apply(DatabendConstant left, DatabendConstant right) {
                return left.isLessThan(right);
            }
        },
        LESS_EQUALS("<=") {
            @Override
            public DatabendConstant apply(DatabendConstant left, DatabendConstant right) {
                DatabendConstant isLessThan = left.isLessThan(right);
                if (isLessThan.isBoolean() && !isLessThan.asBoolean()) {
                    return left.isEquals(right);
                } else {
                    return isLessThan;
                }
            }
        },
        GREATER(">") {
            @Override
            public DatabendConstant apply(DatabendConstant left, DatabendConstant right) {
                DatabendConstant isEquals = left.isEquals(right);
                if (isEquals.isBoolean() && isEquals.asBoolean()) {
                    return DatabendConstant.createBooleanConstant(false);
                } else {
                    DatabendConstant less = left.isLessThan(right);
                    if (less.isNull()) {
                        return DatabendConstant.createNullConstant();
                    }
                    return DatabendConstant.createBooleanConstant(!less.asBoolean());
                }
            }
        },
        GREATER_EQUALS(">=") {
            @Override
            public DatabendConstant apply(DatabendConstant left, DatabendConstant right) {
                DatabendConstant isEquals = left.isEquals(right);
                if (isEquals.isBoolean() && isEquals.asBoolean()) {
                    return DatabendConstant.createBooleanConstant(true);
                } else {
                    DatabendConstant less = left.isLessThan(right);
                    if (less.isNull()) {
                        return DatabendConstant.createNullConstant();
                    }
                    return DatabendConstant.createBooleanConstant(!less.asBoolean());
                }
            }
        };

        private final String textRepresentation;

        DatabendBinaryComparisonOperator(String text) {
            textRepresentation = text;
        }

        public abstract DatabendConstant apply(DatabendConstant left, DatabendConstant right);

        @Override
        public String getTextRepresentation() {
            return textRepresentation;
        }
    }

}
