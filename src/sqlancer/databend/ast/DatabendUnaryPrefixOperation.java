package sqlancer.databend.ast;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.newast.NewUnaryPrefixOperatorNode;
import sqlancer.databend.DatabendExprToNode;
import sqlancer.databend.DatabendSchema.DatabendDataType;

public class DatabendUnaryPrefixOperation extends NewUnaryPrefixOperatorNode<DatabendExpression>
        implements DatabendExpression {

    public DatabendUnaryPrefixOperation(DatabendExpression expr, DatabendUnaryPrefixOperator op) {
        super(DatabendExprToNode.cast(expr), op);
    }

    public DatabendExpression getExpression() {
        return (DatabendExpression) getExpr();
    }

    public DatabendUnaryPrefixOperator getOp() {
        return (DatabendUnaryPrefixOperator) op;
    }

    @Override
    public DatabendDataType getExpectedType() {
        return getOp().getExpressionType(getExpression());
    }

    @Override
    public DatabendConstant getExpectedValue() {
        DatabendConstant expectedValue = getExpression().getExpectedValue();
        if (expectedValue == null) {
            return null;
        }
        return getOp().apply(expectedValue);
    }

    public enum DatabendUnaryPrefixOperator implements BinaryOperatorNode.Operator {
        NOT("NOT", DatabendDataType.BOOLEAN, DatabendDataType.INT) {
            @Override
            public DatabendDataType getExpressionType(DatabendExpression expr) {
                return DatabendDataType.BOOLEAN;
            }

            @Override
            protected DatabendConstant apply(DatabendConstant value) {
                if (value.isNull()) {
                    return DatabendConstant.createNullConstant();
                } else {
                    return DatabendConstant.createBooleanConstant(!value.cast(DatabendDataType.BOOLEAN).asBoolean());
                }
            }
        },

        UNARY_PLUS("+", DatabendDataType.INT) {
            @Override
            public DatabendDataType getExpressionType(DatabendExpression expr) {
                return expr.getExpectedType();
            }

            @Override
            protected DatabendConstant apply(DatabendConstant value) {
                return value;
            }
        },
        UNARY_MINUS("-", DatabendDataType.INT) {
            @Override
            public DatabendDataType getExpressionType(DatabendExpression expr) {
                return expr.getExpectedType();
            }

            @Override
            protected DatabendConstant apply(DatabendConstant value) {
                if (value.isNull()) {
                    return DatabendConstant.createNullConstant();
                }
                try {
                    if (value.isInt()) {
                        return DatabendConstant.createIntConstant(-value.asInt());
                    } else if (value.isFloat()) {
                        return DatabendConstant.createFloatConstant(-value.asFloat());
                    } else {
                        return null;
                    }
                } catch (UnsupportedOperationException e) {
                    return null;
                }
            }
        };

        private String textRepresentation;
        private DatabendDataType[] dataTypes;

        DatabendUnaryPrefixOperator(String textRepresentation, DatabendDataType... dataTypes) {
            this.textRepresentation = textRepresentation;
            this.dataTypes = dataTypes.clone();
        }

        public abstract DatabendDataType getExpressionType(DatabendExpression expr);

        public DatabendDataType getRandomInputDataTypes() {
            return Randomly.fromOptions(dataTypes);
        }

        protected abstract DatabendConstant apply(DatabendConstant value);

        @Override
        public String getTextRepresentation() {
            return this.textRepresentation;
        }
    }

}
