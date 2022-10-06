package sqlancer.yugabyte.ysql.ast;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLDataType;

public class YSQLPostfixOperation implements YSQLExpression {

    private final YSQLExpression expr;
    private final PostfixOperator op;
    private final String operatorTextRepresentation;

    public YSQLPostfixOperation(YSQLExpression expr, PostfixOperator op) {
        this.expr = expr;
        this.operatorTextRepresentation = Randomly.fromOptions(op.textRepresentations);
        this.op = op;
    }

    public static YSQLExpression create(YSQLExpression expr, PostfixOperator op) {
        return new YSQLPostfixOperation(expr, op);
    }

    @Override
    public YSQLDataType getExpressionType() {
        return YSQLDataType.BOOLEAN;
    }

    @Override
    public YSQLConstant getExpectedValue() {
        YSQLConstant expectedValue = expr.getExpectedValue();
        if (expectedValue == null) {
            return null;
        }
        return op.apply(expectedValue);
    }

    public String getOperatorTextRepresentation() {
        return operatorTextRepresentation;
    }

    public YSQLExpression getExpression() {
        return expr;
    }

    public enum PostfixOperator implements Operator {
        IS_NULL("IS NULL", "ISNULL") {
            @Override
            public YSQLConstant apply(YSQLConstant expectedValue) {
                return YSQLConstant.createBooleanConstant(expectedValue.isNull());
            }

            @Override
            public YSQLDataType[] getInputDataTypes() {
                return YSQLDataType.values();
            }

        },
        IS_UNKNOWN("IS UNKNOWN") {
            @Override
            public YSQLConstant apply(YSQLConstant expectedValue) {
                return YSQLConstant.createBooleanConstant(expectedValue.isNull());
            }

            @Override
            public YSQLDataType[] getInputDataTypes() {
                return new YSQLDataType[] { YSQLDataType.BOOLEAN };
            }
        },

        IS_NOT_NULL("IS NOT NULL", "NOTNULL") {
            @Override
            public YSQLConstant apply(YSQLConstant expectedValue) {
                return YSQLConstant.createBooleanConstant(!expectedValue.isNull());
            }

            @Override
            public YSQLDataType[] getInputDataTypes() {
                return YSQLDataType.values();
            }

        },
        IS_NOT_UNKNOWN("IS NOT UNKNOWN") {
            @Override
            public YSQLConstant apply(YSQLConstant expectedValue) {
                return YSQLConstant.createBooleanConstant(!expectedValue.isNull());
            }

            @Override
            public YSQLDataType[] getInputDataTypes() {
                return new YSQLDataType[] { YSQLDataType.BOOLEAN };
            }
        },
        IS_TRUE("IS TRUE") {
            @Override
            public YSQLConstant apply(YSQLConstant expectedValue) {
                if (expectedValue.isNull()) {
                    return YSQLConstant.createFalse();
                } else {
                    return YSQLConstant.createBooleanConstant(expectedValue.cast(YSQLDataType.BOOLEAN).asBoolean());
                }
            }

            @Override
            public YSQLDataType[] getInputDataTypes() {
                return new YSQLDataType[] { YSQLDataType.BOOLEAN };
            }

        },
        IS_FALSE("IS FALSE") {
            @Override
            public YSQLConstant apply(YSQLConstant expectedValue) {
                if (expectedValue.isNull()) {
                    return YSQLConstant.createFalse();
                } else {
                    return YSQLConstant.createBooleanConstant(!expectedValue.cast(YSQLDataType.BOOLEAN).asBoolean());
                }
            }

            @Override
            public YSQLDataType[] getInputDataTypes() {
                return new YSQLDataType[] { YSQLDataType.BOOLEAN };
            }

        };

        private final String[] textRepresentations;

        PostfixOperator(String... textRepresentations) {
            this.textRepresentations = textRepresentations.clone();
        }

        public static PostfixOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        public abstract YSQLConstant apply(YSQLConstant expectedValue);

        public abstract YSQLDataType[] getInputDataTypes();

        @Override
        public String getTextRepresentation() {
            return toString();
        }
    }

}
