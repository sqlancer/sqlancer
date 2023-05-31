package sqlancer.doris.ast;

import java.util.function.BinaryOperator;

import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.newast.NewBinaryOperatorNode;
import sqlancer.doris.DorisSchema.DorisDataType;
import sqlancer.doris.visitor.DorisExprToNode;

public class DorisBinaryArithmeticOperation extends NewBinaryOperatorNode<DorisExpression> implements DorisExpression {

    public DorisBinaryArithmeticOperation(DorisExpression left, DorisExpression right, BinaryOperatorNode.Operator op) {
        super(DorisExprToNode.cast(left), DorisExprToNode.cast(right), op);
    }

    public enum DorisBinaryArithmeticOperator implements BinaryOperatorNode.Operator {
        ADDITION("+") {
            @Override
            public DorisConstant apply(DorisConstant left, DorisConstant right) {
                return applyOperation(left, right, (l, r) -> l + r);
            }
        },
        SUBTRACTION("-") {
            @Override
            public DorisConstant apply(DorisConstant left, DorisConstant right) {
                return applyOperation(left, right, (l, r) -> l - r);
            }
        },
        MULTIPLICATION("*") {
            @Override
            public DorisConstant apply(DorisConstant left, DorisConstant right) {
                return applyOperation(left, right, (l, r) -> l * r);
            }
        },
        DIVISION("/") {
            @Override
            public DorisConstant apply(DorisConstant left, DorisConstant right) {
                return applyOperation(left, right, (l, r) -> r == 0 ? -1 : l / r);
            }
        },
        MODULO("%") {
            @Override
            public DorisConstant apply(DorisConstant left, DorisConstant right) {
                return applyOperation(left, right, (l, r) -> r == 0 ? -1 : l % r);
            }
        },
        CONCAT("||") {
            @Override
            public DorisConstant apply(DorisConstant left, DorisConstant right) {
                if (!left.isBoolean() || !right.isBoolean()) {
                    return DorisConstant.createNullConstant();
                }
                return applyOperation(left, right, (l, r) -> l == 1 || r == 1 ? 1.0 : 0.0);
            }
        },
        BIT_AND("&") {
            @Override
            public DorisConstant apply(DorisConstant left, DorisConstant right) {
                if (!left.isInt() || !right.isInt()) {
                    return DorisConstant.createNullConstant();
                }
                return applyOperation(left, right, (l, r) -> (double) ((int) l.doubleValue() & (int) r.doubleValue()));
            }
        },
        BIT_OR("|") {
            @Override
            public DorisConstant apply(DorisConstant left, DorisConstant right) {
                if (!left.isInt() || !right.isInt()) {
                    return DorisConstant.createNullConstant();
                }
                return applyOperation(left, right, (l, r) -> (double) ((int) l.doubleValue() | (int) r.doubleValue()));
            }
        },
        LSHIFT("<<") {
            @Override
            public DorisConstant apply(DorisConstant left, DorisConstant right) {
                if (!left.isInt() || !right.isInt()) {
                    return DorisConstant.createNullConstant();
                }
                return applyOperation(left, right, (l, r) -> (double) ((int) l.doubleValue() << (int) r.doubleValue()));
            }
        },
        RSHIFT(">>") {
            @Override
            public DorisConstant apply(DorisConstant left, DorisConstant right) {
                if (!left.isInt() || !right.isInt()) {
                    return DorisConstant.createNullConstant();
                }
                return applyOperation(left, right, (l, r) -> (double) ((int) l.doubleValue() >> (int) r.doubleValue()));
            }
        };

        private final String textRepresentation;

        DorisBinaryArithmeticOperator(String text) {
            textRepresentation = text;
        }

        public abstract DorisConstant apply(DorisConstant left, DorisConstant right);

        public DorisConstant applyOperation(DorisConstant left, DorisConstant right, BinaryOperator<Double> op) {
            if (left.isNull() || right.isNull()) {
                return DorisConstant.createNullConstant();
            }
            double leftVal = left.cast(DorisDataType.FLOAT).asFloat();
            double rightVal = right.cast(DorisDataType.FLOAT).asFloat();
            return DorisConstant.createFloatConstant(op.apply(leftVal, rightVal));
        }

        @Override
        public String getTextRepresentation() {
            return textRepresentation;
        }
    }

    public DorisExpression getLeftExpr() {
        return (DorisExpression) super.getLeft();
    }

    public DorisExpression getRightExpr() {
        return (DorisExpression) super.getRight();
    }

    public DorisBinaryArithmeticOperator getOp() {
        return (DorisBinaryArithmeticOperator) op;
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
        return DorisDataType.FLOAT;
    }

}
