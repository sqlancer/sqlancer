package sqlancer.postgres.ast;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.postgres.PostgresSchema.PostgresDataType;

public class PostgresPostfixOperation implements PostgresExpression {

    private final PostgresExpression expr;
    private final PostfixOperator op;
    private final String operatorTextRepresentation;

    public enum PostfixOperator implements Operator {
        IS_NULL("IS NULL", "ISNULL") {
            @Override
            public PostgresConstant apply(PostgresConstant expectedValue) {
                return PostgresConstant.createBooleanConstant(expectedValue.isNull());
            }

            @Override
            public PostgresDataType[] getInputDataTypes() {
                return PostgresDataType.values();
            }

        },
        IS_UNKNOWN("IS UNKNOWN") {
            @Override
            public PostgresConstant apply(PostgresConstant expectedValue) {
                return PostgresConstant.createBooleanConstant(expectedValue.isNull());
            }

            @Override
            public PostgresDataType[] getInputDataTypes() {
                return new PostgresDataType[] { PostgresDataType.BOOLEAN };
            }
        },

        IS_NOT_NULL("IS NOT NULL", "NOTNULL") {

            @Override
            public PostgresConstant apply(PostgresConstant expectedValue) {
                return PostgresConstant.createBooleanConstant(!expectedValue.isNull());
            }

            @Override
            public PostgresDataType[] getInputDataTypes() {
                return PostgresDataType.values();
            }

        },
        IS_NOT_UNKNOWN("IS NOT UNKNOWN") {
            @Override
            public PostgresConstant apply(PostgresConstant expectedValue) {
                return PostgresConstant.createBooleanConstant(!expectedValue.isNull());
            }

            @Override
            public PostgresDataType[] getInputDataTypes() {
                return new PostgresDataType[] { PostgresDataType.BOOLEAN };
            }
        },
        IS_TRUE("IS TRUE") {

            @Override
            public PostgresConstant apply(PostgresConstant expectedValue) {
                if (expectedValue.isNull()) {
                    return PostgresConstant.createFalse();
                } else {
                    return PostgresConstant
                            .createBooleanConstant(expectedValue.cast(PostgresDataType.BOOLEAN).asBoolean());
                }
            }

            @Override
            public PostgresDataType[] getInputDataTypes() {
                return new PostgresDataType[] { PostgresDataType.BOOLEAN };
            }

        },
        IS_FALSE("IS FALSE") {

            @Override
            public PostgresConstant apply(PostgresConstant expectedValue) {
                if (expectedValue.isNull()) {
                    return PostgresConstant.createFalse();
                } else {
                    return PostgresConstant
                            .createBooleanConstant(!expectedValue.cast(PostgresDataType.BOOLEAN).asBoolean());
                }
            }

            @Override
            public PostgresDataType[] getInputDataTypes() {
                return new PostgresDataType[] { PostgresDataType.BOOLEAN };
            }

        };

        private String[] textRepresentations;

        PostfixOperator(String... textRepresentations) {
            this.textRepresentations = textRepresentations.clone();
        }

        public abstract PostgresConstant apply(PostgresConstant expectedValue);

        public abstract PostgresDataType[] getInputDataTypes();

        public static PostfixOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return toString();
        }
    }

    public PostgresPostfixOperation(PostgresExpression expr, PostfixOperator op) {
        this.expr = expr;
        this.operatorTextRepresentation = Randomly.fromOptions(op.textRepresentations);
        this.op = op;
    }

    @Override
    public PostgresDataType getExpressionType() {
        return PostgresDataType.BOOLEAN;
    }

    @Override
    public PostgresConstant getExpectedValue() {
        PostgresConstant expectedValue = expr.getExpectedValue();
        if (expectedValue == null) {
            return null;
        }
        return op.apply(expectedValue);
    }

    public String getOperatorTextRepresentation() {
        return operatorTextRepresentation;
    }

    public static PostgresExpression create(PostgresExpression expr, PostfixOperator op) {
        return new PostgresPostfixOperation(expr, op);
    }

    public PostgresExpression getExpression() {
        return expr;
    }

}
