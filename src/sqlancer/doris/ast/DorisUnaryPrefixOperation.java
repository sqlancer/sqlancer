package sqlancer.doris.ast;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.newast.NewUnaryPrefixOperatorNode;
import sqlancer.doris.DorisSchema.DorisDataType;
import sqlancer.doris.visitor.DorisExprToNode;

public class DorisUnaryPrefixOperation extends NewUnaryPrefixOperatorNode<DorisExpression> implements DorisExpression {

    public DorisUnaryPrefixOperation(DorisExpression expr, DorisUnaryPrefixOperator op) {
        super(DorisExprToNode.cast(expr), op);
    }

    public DorisExpression getExpression() {
        return (DorisExpression) getExpr();
    }

    public DorisUnaryPrefixOperator getOp() {
        return (DorisUnaryPrefixOperator) op;
    }

    @Override
    public DorisDataType getExpectedType() {
        return getOp().getExpressionType(getExpression());
    }

    @Override
    public DorisConstant getExpectedValue() {
        DorisConstant expectedValue = getExpression().getExpectedValue();
        if (expectedValue == null) {
            return null;
        }
        return getOp().apply(expectedValue);
    }

    public enum DorisUnaryPrefixOperator implements BinaryOperatorNode.Operator {
        NOT("NOT", DorisDataType.BOOLEAN, DorisDataType.INT) {
            @Override
            public DorisDataType getExpressionType(DorisExpression expr) {
                return DorisDataType.BOOLEAN;
            }

            @Override
            protected DorisConstant apply(DorisConstant value) {
                if (value.isNull()) {
                    return DorisConstant.createNullConstant();
                } else {
                    return DorisConstant.createBooleanConstant(!value.cast(DorisDataType.BOOLEAN).asBoolean());
                }
            }
        },

        UNARY_PLUS("+", DorisDataType.INT) {
            @Override
            public DorisDataType getExpressionType(DorisExpression expr) {
                return expr.getExpectedType();
            }

            @Override
            protected DorisConstant apply(DorisConstant value) {
                return value;
            }
        },
        UNARY_MINUS("-", DorisDataType.INT) {
            @Override
            public DorisDataType getExpressionType(DorisExpression expr) {
                return expr.getExpectedType();
            }

            @Override
            protected DorisConstant apply(DorisConstant value) {
                if (value.isNull()) {
                    return DorisConstant.createNullConstant();
                }
                try {
                    if (value.isInt()) {
                        return DorisConstant.createIntConstant(-value.asInt());
                    }
                    if (value.isFloat()) {
                        return DorisConstant.createFloatConstant(-value.asFloat());
                    }
                    return null;
                } catch (UnsupportedOperationException e) {
                    return null;
                }
            }
        };

        private String textRepresentation;
        private DorisDataType[] dataTypes;

        DorisUnaryPrefixOperator(String textRepresentation, DorisDataType... dataTypes) {
            this.textRepresentation = textRepresentation;
            this.dataTypes = dataTypes.clone();
        }

        public abstract DorisDataType getExpressionType(DorisExpression expr);

        public DorisDataType getRandomInputDataTypes() {
            return Randomly.fromOptions(dataTypes);
        }

        protected abstract DorisConstant apply(DorisConstant value);

        @Override
        public String getTextRepresentation() {
            return this.textRepresentation;
        }
    }

}
