package sqlancer.postgres.ast;

import sqlancer.IgnoreMeException;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.postgres.PostgresSchema.PostgresDataType;

public class PostgresPrefixOperation implements PostgresExpression {

    public enum PrefixOperator implements Operator {
        NOT("NOT", PostgresDataType.BOOLEAN) {

            @Override
            public PostgresDataType getExpressionType() {
                return PostgresDataType.BOOLEAN;
            }

            @Override
            protected PostgresConstant getExpectedValue(PostgresConstant expectedValue) {
                if (expectedValue.isNull()) {
                    return PostgresConstant.createNullConstant();
                } else {
                    return PostgresConstant
                            .createBooleanConstant(!expectedValue.cast(PostgresDataType.BOOLEAN).asBoolean());
                }
            }
        },
        UNARY_PLUS("+", PostgresDataType.INT) {

            @Override
            public PostgresDataType getExpressionType() {
                return PostgresDataType.INT;
            }

            @Override
            protected PostgresConstant getExpectedValue(PostgresConstant expectedValue) {
                // TODO: actual converts to double precision
                return expectedValue;
            }

        },
        UNARY_MINUS("-", PostgresDataType.INT) {

            @Override
            public PostgresDataType getExpressionType() {
                return PostgresDataType.INT;
            }

            @Override
            protected PostgresConstant getExpectedValue(PostgresConstant expectedValue) {
                if (expectedValue.isNull()) {
                    // TODO
                    throw new IgnoreMeException();
                }
                if (expectedValue.isInt() && expectedValue.asInt() == Long.MIN_VALUE) {
                    throw new IgnoreMeException();
                }
                try {
                    return PostgresConstant.createIntConstant(-expectedValue.asInt());
                } catch (UnsupportedOperationException e) {
                    return null;
                }
            }

        };

        private String textRepresentation;
        private PostgresDataType[] dataTypes;

        PrefixOperator(String textRepresentation, PostgresDataType... dataTypes) {
            this.textRepresentation = textRepresentation;
            this.dataTypes = dataTypes.clone();
        }

        public abstract PostgresDataType getExpressionType();

        protected abstract PostgresConstant getExpectedValue(PostgresConstant expectedValue);

        @Override
        public String getTextRepresentation() {
            return toString();
        }

    }

    private final PostgresExpression expr;
    private final PrefixOperator op;

    public PostgresPrefixOperation(PostgresExpression expr, PrefixOperator op) {
        this.expr = expr;
        this.op = op;
    }

    @Override
    public PostgresDataType getExpressionType() {
        return op.getExpressionType();
    }

    @Override
    public PostgresConstant getExpectedValue() {
        PostgresConstant expectedValue = expr.getExpectedValue();
        if (expectedValue == null) {
            return null;
        }
        return op.getExpectedValue(expectedValue);
    }

    public PostgresDataType[] getInputDataTypes() {
        return op.dataTypes;
    }

    public String getTextRepresentation() {
        return op.textRepresentation;
    }

    public PostgresExpression getExpression() {
        return expr;
    }

}
