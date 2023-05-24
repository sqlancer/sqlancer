package sqlancer.doris.ast;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.newast.NewBinaryOperatorNode;
import sqlancer.doris.DorisSchema.DorisDataType;
import sqlancer.doris.visitor.DorisExprToNode;

public class DorisBinaryLogicalOperation extends NewBinaryOperatorNode<DorisExpression> implements DorisExpression {

    public DorisBinaryLogicalOperation(DorisExpression left, DorisExpression right, DorisBinaryLogicalOperator op) {
        super(DorisExprToNode.cast(left), DorisExprToNode.cast(right), op);
    }

    public DorisExpression getLeftExpr() {
        return (DorisExpression) super.getLeft();
    }

    public DorisExpression getRightExpr() {
        return (DorisExpression) super.getRight();
    }

    public DorisBinaryLogicalOperator getOp() {
        return (DorisBinaryLogicalOperator) op;
    }

    @Override
    public DorisConstant getExpectedValue() {
        DorisConstant leftValue = getLeftExpr().getExpectedValue();
        DorisConstant rightValue = getRightExpr().getExpectedValue();
        if (leftValue == null || rightValue == null) {
            return null;
        }
        return getOp().apply(leftValue, rightValue);
    }

    @Override
    public DorisDataType getExpectedType() {
        return DorisDataType.BOOLEAN;
    }

    public enum DorisBinaryLogicalOperator implements BinaryOperatorNode.Operator {
        /*
         * null and false -> false null and true -> null null or false -> null null or true -> true
         */
        AND("AND", "and") {
            @Override
            public DorisConstant apply(DorisConstant left, DorisConstant right) {
                DorisConstant leftVal = left.cast(DorisDataType.BOOLEAN);
                DorisConstant rightVal = right.cast(DorisDataType.BOOLEAN);
                assert leftVal.isNull() || leftVal.isBoolean() : leftVal + "is not null or boolean";
                assert rightVal.isNull() || rightVal.isBoolean() : rightVal + "is not null or boolean";
                if (leftVal.isNull() && rightVal.isNull()) {
                    return DorisConstant.createNullConstant();
                }
                if (leftVal.isNull()) {
                    if (!rightVal.asBoolean()) {
                        return DorisConstant.createBooleanConstant(false);
                    } else {
                        return DorisConstant.createNullConstant();
                    }
                }
                if (rightVal.isNull()) {
                    if (!leftVal.asBoolean()) {
                        return DorisConstant.createBooleanConstant(false);
                    } else {
                        return DorisConstant.createNullConstant();
                    }
                }
                if (leftVal.asBoolean() && right.asBoolean()) {
                    return DorisConstant.createBooleanConstant(true);
                }
                return DorisConstant.createBooleanConstant(false);
            }
        },
        OR("OR", "or") {
            @Override
            public DorisConstant apply(DorisConstant left, DorisConstant right) {
                DorisConstant leftVal = left.cast(DorisDataType.BOOLEAN);
                DorisConstant rightVal = right.cast(DorisDataType.BOOLEAN);
                assert leftVal.isNull() || leftVal.isBoolean() : leftVal + "is not null or boolean";
                assert rightVal.isNull() || rightVal.isBoolean() : rightVal + "is not null or boolean";
                if (leftVal.isNull() && rightVal.isNull()) {
                    return DorisConstant.createNullConstant();
                }
                if (leftVal.isNull()) {
                    if (rightVal.asBoolean()) {
                        return DorisConstant.createBooleanConstant(true);
                    } else {
                        return DorisConstant.createNullConstant();
                    }
                }
                if (rightVal.isNull()) {
                    if (leftVal.asBoolean()) {
                        return DorisConstant.createBooleanConstant(true);
                    } else {
                        return DorisConstant.createNullConstant();
                    }
                }
                if (leftVal.asBoolean() || right.asBoolean()) {
                    return DorisConstant.createBooleanConstant(true);
                }
                return DorisConstant.createBooleanConstant(false);
            }
        };

        private final String[] textRepresentations;

        DorisBinaryLogicalOperator(String... textRepresentations) {
            this.textRepresentations = textRepresentations.clone();
        }

        @Override
        public String getTextRepresentation() {
            return Randomly.fromOptions(textRepresentations);
        }

        public DorisBinaryLogicalOperator getRandomOp() {
            return Randomly.fromOptions(values());
        }

        public static DorisBinaryLogicalOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        public abstract DorisConstant apply(DorisConstant left, DorisConstant right);

    }

}
