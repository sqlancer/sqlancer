package sqlancer.databend.ast;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.newast.NewUnaryPostfixOperatorNode;
import sqlancer.databend.DatabendExprToNode;
import sqlancer.databend.DatabendSchema.DatabendDataType;

public class DatabendUnaryPostfixOperation extends NewUnaryPostfixOperatorNode<DatabendExpression>
        implements DatabendExpression {

    public DatabendUnaryPostfixOperation(DatabendExpression expr, DatabendUnaryPostfixOperator op) {
        super(DatabendExprToNode.cast(expr), op);
    }

    public DatabendExpression getExpression() {
        return (DatabendExpression) getExpr();
    }

    public DatabendUnaryPostfixOperator getOp() {
        return (DatabendUnaryPostfixOperator) op;
    }

    @Override
    public DatabendDataType getExpectedType() {
        return DatabendDataType.BOOLEAN;
    }

    @Override
    public DatabendConstant getExpectedValue() {
        DatabendConstant expectedValue = getExpression().getExpectedValue();
        if (expectedValue == null) {
            return null;
        }
        return getOp().apply(expectedValue);
    }

    public enum DatabendUnaryPostfixOperator implements BinaryOperatorNode.Operator {
        IS_NULL("IS NULL") {
            @Override
            public DatabendDataType[] getInputDataTypes() {
                return DatabendDataType.values();
            }

            @Override
            public DatabendConstant apply(DatabendConstant value) {
                return DatabendConstant.createBooleanConstant(value.isNull());
            }
        },
        IS_NOT_NULL("IS NOT NULL") {
            @Override
            public DatabendDataType[] getInputDataTypes() {
                return DatabendDataType.values();
            }

            @Override
            public DatabendConstant apply(DatabendConstant value) {
                return DatabendConstant.createBooleanConstant(!value.isNull());
            }
        };
        // IS

        private final String textRepresentations;

        DatabendUnaryPostfixOperator(String text) {
            this.textRepresentations = text;
        }

        public static DatabendUnaryPostfixOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return textRepresentations;
        }

        public abstract DatabendDataType[] getInputDataTypes();

        public abstract DatabendConstant apply(DatabendConstant value);
    }

    @Override
    public String getOperatorRepresentation() {
        return this.op.getTextRepresentation();
    }

}
