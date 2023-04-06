package sqlancer.materialize.ast;

import sqlancer.IgnoreMeException;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.materialize.MaterializeSchema.MaterializeDataType;

public class MaterializePrefixOperation implements MaterializeExpression {

    public enum PrefixOperator implements Operator {
        NOT("NOT", MaterializeDataType.BOOLEAN) {

            @Override
            public MaterializeDataType getExpressionType() {
                return MaterializeDataType.BOOLEAN;
            }

            @Override
            protected MaterializeConstant getExpectedValue(MaterializeConstant expectedValue) {
                if (expectedValue.isNull()) {
                    return MaterializeConstant.createNullConstant();
                } else {
                    return MaterializeConstant
                            .createBooleanConstant(!expectedValue.cast(MaterializeDataType.BOOLEAN).asBoolean());
                }
            }
        },
        UNARY_PLUS("+", MaterializeDataType.INT) {

            @Override
            public MaterializeDataType getExpressionType() {
                return MaterializeDataType.INT;
            }

            @Override
            protected MaterializeConstant getExpectedValue(MaterializeConstant expectedValue) {
                // TODO: actual converts to double precision
                return expectedValue;
            }

        },
        UNARY_MINUS("-", MaterializeDataType.INT) {

            @Override
            public MaterializeDataType getExpressionType() {
                return MaterializeDataType.INT;
            }

            @Override
            protected MaterializeConstant getExpectedValue(MaterializeConstant expectedValue) {
                if (expectedValue.isNull()) {
                    // TODO
                    throw new IgnoreMeException();
                }
                if (expectedValue.isInt() && expectedValue.asInt() == Long.MIN_VALUE) {
                    throw new IgnoreMeException();
                }
                try {
                    return MaterializeConstant.createIntConstant(-expectedValue.asInt());
                } catch (UnsupportedOperationException e) {
                    return null;
                }
            }

        };

        private String textRepresentation;
        private MaterializeDataType[] dataTypes;

        PrefixOperator(String textRepresentation, MaterializeDataType... dataTypes) {
            this.textRepresentation = textRepresentation;
            this.dataTypes = dataTypes.clone();
        }

        public abstract MaterializeDataType getExpressionType();

        protected abstract MaterializeConstant getExpectedValue(MaterializeConstant expectedValue);

        @Override
        public String getTextRepresentation() {
            return toString();
        }

    }

    private final MaterializeExpression expr;
    private final PrefixOperator op;

    public MaterializePrefixOperation(MaterializeExpression expr, PrefixOperator op) {
        this.expr = expr;
        this.op = op;
    }

    @Override
    public MaterializeDataType getExpressionType() {
        return op.getExpressionType();
    }

    @Override
    public MaterializeConstant getExpectedValue() {
        MaterializeConstant expectedValue = expr.getExpectedValue();
        if (expectedValue == null) {
            return null;
        }
        return op.getExpectedValue(expectedValue);
    }

    public MaterializeDataType[] getInputDataTypes() {
        return op.dataTypes;
    }

    public String getTextRepresentation() {
        return op.textRepresentation;
    }

    public MaterializeExpression getExpression() {
        return expr;
    }

}
