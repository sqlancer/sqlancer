package sqlancer.databend.ast;

import java.util.function.BinaryOperator;

import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.newast.NewBinaryOperatorNode;
import sqlancer.databend.DatabendExprToNode;
import sqlancer.databend.DatabendSchema.DatabendDataType;

public class DatabendBinaryArithmeticOperation extends NewBinaryOperatorNode<DatabendExpression>
        implements DatabendExpression {

    public DatabendBinaryArithmeticOperation(DatabendExpression left, DatabendExpression right,
            BinaryOperatorNode.Operator op) {
        super(DatabendExprToNode.cast(left), DatabendExprToNode.cast(right), op);
    }

    public enum DatabendBinaryArithmeticOperator implements BinaryOperatorNode.Operator {
        ADDITION("+") {
            @Override
            public DatabendConstant apply(DatabendConstant left, DatabendConstant right) {
                return applyOperation(left, right, (l, r) -> l + r);
            }
        },
        SUBTRACTION("-") {
            @Override
            public DatabendConstant apply(DatabendConstant left, DatabendConstant right) {
                return applyOperation(left, right, (l, r) -> l - r);
            }
        },
        MULTIPLICATION("*") {
            @Override
            public DatabendConstant apply(DatabendConstant left, DatabendConstant right) {
                return applyOperation(left, right, (l, r) -> l * r);
            }
        },
        DIVISION("/") {
            @Override
            public DatabendConstant apply(DatabendConstant left, DatabendConstant right) {
                return applyOperation(left, right, (l, r) -> r == 0 ? -1 : l / r);
            }
        },
        MODULO("%") {
            @Override
            public DatabendConstant apply(DatabendConstant left, DatabendConstant right) {
                return applyOperation(left, right, (l, r) -> r == 0 ? -1 : l % r);
            }
        };

        private final String textRepresentation;

        DatabendBinaryArithmeticOperator(String text) {
            textRepresentation = text;
        }

        public abstract DatabendConstant apply(DatabendConstant left, DatabendConstant right);

        public DatabendConstant applyOperation(DatabendConstant left, DatabendConstant right, BinaryOperator<Long> op) {
            if (left.isNull() || right.isNull()) {
                return DatabendConstant.createNullConstant();
            } else {
                long leftVal = left.cast(DatabendDataType.INT).asInt();
                long rightVal = right.cast(DatabendDataType.INT).asInt();
                return DatabendConstant.createIntConstant(op.apply(leftVal, rightVal));
            }
        }

        @Override
        public String getTextRepresentation() {
            return textRepresentation;
        }
    }

    public DatabendExpression getLeftExpr() {
        return (DatabendExpression) super.getLeft();
    }

    public DatabendExpression getRightExpr() {
        return (DatabendExpression) super.getRight();
    }

    public DatabendBinaryArithmeticOperator getOp() {
        return (DatabendBinaryArithmeticOperator) op;
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
        return DatabendDataType.INT;
    }

}
