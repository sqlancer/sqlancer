package sqlancer.yugabyte.ysql.ast;

import sqlancer.IgnoreMeException;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLDataType;

public class YSQLPrefixOperation implements YSQLExpression {

    private final YSQLExpression expr;
    private final PrefixOperator op;

    public YSQLPrefixOperation(YSQLExpression expr, PrefixOperator op) {
        this.expr = expr;
        this.op = op;
    }

    @Override
    public YSQLDataType getExpressionType() {
        return op.getExpressionType();
    }

    @Override
    public YSQLConstant getExpectedValue() {
        YSQLConstant expectedValue = expr.getExpectedValue();
        if (expectedValue == null) {
            return null;
        }
        return op.getExpectedValue(expectedValue);
    }

    public YSQLDataType[] getInputDataTypes() {
        return op.dataTypes;
    }

    public String getTextRepresentation() {
        return op.textRepresentation;
    }

    public YSQLExpression getExpression() {
        return expr;
    }

    public enum PrefixOperator implements Operator {
        NOT("NOT", YSQLDataType.BOOLEAN) {
            @Override
            public YSQLDataType getExpressionType() {
                return YSQLDataType.BOOLEAN;
            }

            @Override
            protected YSQLConstant getExpectedValue(YSQLConstant expectedValue) {
                if (expectedValue.isNull()) {
                    return YSQLConstant.createNullConstant();
                } else {
                    return YSQLConstant.createBooleanConstant(!expectedValue.cast(YSQLDataType.BOOLEAN).asBoolean());
                }
            }
        },
        UNARY_PLUS("+", YSQLDataType.INT) {
            @Override
            public YSQLDataType getExpressionType() {
                return YSQLDataType.INT;
            }

            @Override
            protected YSQLConstant getExpectedValue(YSQLConstant expectedValue) {
                // TODO: actual converts to double precision
                return expectedValue;
            }

        },
        UNARY_MINUS("-", YSQLDataType.INT) {
            @Override
            public YSQLDataType getExpressionType() {
                return YSQLDataType.INT;
            }

            @Override
            protected YSQLConstant getExpectedValue(YSQLConstant expectedValue) {
                if (expectedValue.isNull()) {
                    // TODO
                    throw new IgnoreMeException();
                }
                if (expectedValue.isInt() && expectedValue.asInt() == Long.MIN_VALUE) {
                    throw new IgnoreMeException();
                }
                try {
                    return YSQLConstant.createIntConstant(-expectedValue.asInt());
                } catch (UnsupportedOperationException e) {
                    return null;
                }
            }

        };

        private final String textRepresentation;
        private final YSQLDataType[] dataTypes;

        PrefixOperator(String textRepresentation, YSQLDataType... dataTypes) {
            this.textRepresentation = textRepresentation;
            this.dataTypes = dataTypes.clone();
        }

        public abstract YSQLDataType getExpressionType();

        protected abstract YSQLConstant getExpectedValue(YSQLConstant expectedValue);

        @Override
        public String getTextRepresentation() {
            return toString();
        }

    }

}
