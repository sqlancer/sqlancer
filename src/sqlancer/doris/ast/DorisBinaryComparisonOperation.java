package sqlancer.doris.ast;

import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.newast.NewBinaryOperatorNode;
import sqlancer.doris.DorisSchema.DorisDataType;
import sqlancer.doris.visitor.DorisExprToNode;

public class DorisBinaryComparisonOperation extends NewBinaryOperatorNode<DorisExpression> implements DorisExpression {

    public DorisBinaryComparisonOperation(DorisExpression left, DorisExpression right,
            DorisBinaryComparisonOperator op) {
        super(DorisExprToNode.cast(left), DorisExprToNode.cast(right), op);
    }

    public DorisExpression getLeftExpression() {
        return (DorisExpression) super.getLeft();
    }

    public DorisExpression getRightExpression() {
        return (DorisExpression) super.getRight();
    }

    public DorisBinaryComparisonOperator getOp() {
        return (DorisBinaryComparisonOperator) op;
    }

    @Override
    public DorisDataType getExpectedType() {
        return DorisDataType.BOOLEAN;
    }

    @Override
    public DorisConstant getExpectedValue() {
        DorisConstant leftExpectedValue = getLeftExpression().getExpectedValue();
        DorisConstant rightExpectedValue = getRightExpression().getExpectedValue();
        if (leftExpectedValue == null || rightExpectedValue == null) {
            return null;
        }
        return getOp().apply(leftExpectedValue, rightExpectedValue);
    }

    public enum DorisBinaryComparisonOperator implements BinaryOperatorNode.Operator {
        EQUALS("=") {
            @Override
            public DorisConstant apply(DorisConstant left, DorisConstant right) {
                return left.valueEquals(right);
            }
        },
        NOT_EQUALS("!=") {
            @Override
            public DorisConstant apply(DorisConstant left, DorisConstant right) {
                DorisConstant valueEquals = left.valueEquals(right);
                if (valueEquals.isBoolean()) {
                    return DorisConstant.createBooleanConstant(!valueEquals.asBoolean());
                }
                // maybe DorisNULLConstant or null object
                return valueEquals;
            }
        },
        LESS("<") {
            @Override
            public DorisConstant apply(DorisConstant left, DorisConstant right) {
                return left.valueLessThan(right);
            }
        },
        LESS_EQUALS("<=") {
            @Override
            public DorisConstant apply(DorisConstant left, DorisConstant right) {
                DorisConstant valueLessThan = left.valueLessThan(right);
                DorisConstant valueEquals = left.valueEquals(right);
                if (valueEquals.isBoolean() && valueEquals.asBoolean()) {
                    return valueEquals;
                }
                return valueLessThan;
            }
        },
        GREATER(">") {
            @Override
            public DorisConstant apply(DorisConstant left, DorisConstant right) {
                DorisConstant valueLessThan = left.valueLessThan(right);
                DorisConstant valueEquals = left.valueEquals(right);
                if (valueEquals.isBoolean() && valueEquals.asBoolean()) {
                    return DorisConstant.createBooleanConstant(false);
                }
                if (valueLessThan.isNull()) {
                    return valueLessThan;
                }
                return DorisConstant.createBooleanConstant(!valueLessThan.asBoolean());
            }
        },
        GREATER_EQUALS(">=") {
            @Override
            public DorisConstant apply(DorisConstant left, DorisConstant right) {
                DorisConstant valueLessThan = left.valueLessThan(right);
                DorisConstant valueEquals = left.valueEquals(right);
                if (valueEquals.isBoolean() && valueEquals.asBoolean()) {
                    return DorisConstant.createBooleanConstant(true);
                }
                if (valueLessThan.isNull()) {
                    return valueLessThan;
                }
                return DorisConstant.createBooleanConstant(!valueLessThan.asBoolean());
            }
        };

        private final String textRepresentation;

        DorisBinaryComparisonOperator(String text) {
            textRepresentation = text;
        }

        public abstract DorisConstant apply(DorisConstant left, DorisConstant right);

        @Override
        public String getTextRepresentation() {
            return textRepresentation;
        }
    }

}
