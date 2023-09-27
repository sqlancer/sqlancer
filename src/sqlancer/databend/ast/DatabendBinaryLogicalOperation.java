package sqlancer.databend.ast;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.newast.NewBinaryOperatorNode;
import sqlancer.databend.DatabendExprToNode;
import sqlancer.databend.DatabendSchema.DatabendDataType;

public class DatabendBinaryLogicalOperation extends NewBinaryOperatorNode<DatabendExpression>
        implements DatabendExpression {

    public DatabendBinaryLogicalOperation(DatabendExpression left, DatabendExpression right,
            DatabendBinaryLogicalOperator op) {
        super(DatabendExprToNode.cast(left), DatabendExprToNode.cast(right), op);
    }

    public DatabendExpression getLeftExpr() {
        return (DatabendExpression) super.getLeft();
    }

    public DatabendExpression getRightExpr() {
        return (DatabendExpression) super.getRight();
    }

    public DatabendBinaryLogicalOperator getOp() {
        return (DatabendBinaryLogicalOperator) op;
    }

    @Override
    public DatabendConstant getExpectedValue() {
        DatabendConstant leftValue = getLeftExpr().getExpectedValue();
        DatabendConstant rightValue = getRightExpr().getExpectedValue();
        if (leftValue == null || rightValue == null) {
            return null;
        }
        return getOp().apply(leftValue, rightValue);
    }

    @Override
    public DatabendDataType getExpectedType() {
        return DatabendDataType.BOOLEAN;
    }

    public enum DatabendBinaryLogicalOperator implements BinaryOperatorNode.Operator {
        AND("AND", "and") {
            @Override
            public DatabendConstant apply(DatabendConstant left, DatabendConstant right) {
                DatabendConstant leftVal = left.cast(DatabendDataType.BOOLEAN);
                DatabendConstant rightVal = right.cast(DatabendDataType.BOOLEAN);
                assert leftVal.isNull() || leftVal.isBoolean() : leftVal + "不是NULL也不是Boolean类型";
                assert rightVal.isNull() || rightVal.isBoolean() : rightVal + "不是NULL也不是Boolean类型";
                if (leftVal.isNull()) {
                    if (rightVal.isNull()) {
                        return DatabendConstant.createNullConstant();
                    } else {
                        if (rightVal.asBoolean()) {
                            return DatabendConstant.createNullConstant();
                        } else {
                            return DatabendConstant.createBooleanConstant(false);
                        }
                    }
                } else if (!leftVal.asBoolean()) {
                    return DatabendConstant.createBooleanConstant(false);
                }
                assert leftVal.asBoolean();
                if (rightVal.isNull()) {
                    return DatabendConstant.createNullConstant();
                } else {
                    return DatabendConstant.createBooleanConstant(rightVal.asBoolean());
                }
            }
        },
        OR("OR", "or") {
            @Override
            public DatabendConstant apply(DatabendConstant left, DatabendConstant right) {
                DatabendConstant leftVal = left.cast(DatabendDataType.BOOLEAN);
                DatabendConstant rightVal = right.cast(DatabendDataType.BOOLEAN);
                assert leftVal.isNull() || leftVal.isBoolean() : leftVal + "不是NULL也不是Boolean类型";
                assert rightVal.isNull() || rightVal.isBoolean() : rightVal + "不是NULL也不是Boolean类型";
                if (leftVal.isBoolean() && leftVal.asBoolean()) {
                    return DatabendConstant.createBooleanConstant(true);
                }
                if (rightVal.isBoolean() && rightVal.asBoolean()) {
                    return DatabendConstant.createBooleanConstant(true);
                }
                if (leftVal.isNull() || rightVal.isNull()) {
                    return DatabendConstant.createNullConstant();
                }
                return DatabendConstant.createBooleanConstant(false);
            }
        };

        private final String[] textRepresentations;

        DatabendBinaryLogicalOperator(String... textRepresentations) {
            this.textRepresentations = textRepresentations.clone();
        }

        @Override
        public String getTextRepresentation() {
            return Randomly.fromOptions(textRepresentations);
        }

        public DatabendBinaryLogicalOperator getRandomOp() {
            return Randomly.fromOptions(values());
        }

        public static DatabendBinaryLogicalOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        public abstract DatabendConstant apply(DatabendConstant left, DatabendConstant right);

    }

}
