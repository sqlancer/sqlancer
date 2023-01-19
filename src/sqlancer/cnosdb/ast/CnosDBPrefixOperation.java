package sqlancer.cnosdb.ast;

import sqlancer.IgnoreMeException;
import sqlancer.cnosdb.CnosDBSchema.CnosDBDataType;
import sqlancer.common.ast.BinaryOperatorNode.Operator;

public class CnosDBPrefixOperation implements CnosDBExpression {

    private final CnosDBExpression expr;
    private final PrefixOperator op;

    public CnosDBPrefixOperation(CnosDBExpression expr, PrefixOperator op) {
        this.expr = expr;
        this.op = op;
    }

    @Override
    public CnosDBDataType getExpressionType() {
        return op.getExpressionType();
    }

    @Override
    public CnosDBConstant getExpectedValue() {
        CnosDBConstant expectedValue = expr.getExpectedValue();
        if (expectedValue == null) {
            return null;
        }
        return op.getExpectedValue(expectedValue);
    }

    public CnosDBDataType[] getInputDataTypes() {
        return op.dataTypes;
    }

    public String getTextRepresentation() {
        return op.textRepresentation;
    }

    public CnosDBExpression getExpression() {
        return expr;
    }

    public enum PrefixOperator implements Operator {
        NOT("NOT", CnosDBDataType.BOOLEAN) {
            @Override
            public CnosDBDataType getExpressionType() {
                return CnosDBDataType.BOOLEAN;
            }

            @Override
            protected CnosDBConstant getExpectedValue(CnosDBConstant expectedValue) {
                if (expectedValue.isNull()) {
                    return CnosDBConstant.createNullConstant();
                } else {
                    return CnosDBConstant
                            .createBooleanConstant(!expectedValue.cast(CnosDBDataType.BOOLEAN).asBoolean());
                }
            }
        },
        UNARY_PLUS("+", CnosDBDataType.INT) {
            @Override
            public CnosDBDataType getExpressionType() {
                return CnosDBDataType.INT;
            }

            @Override
            protected CnosDBConstant getExpectedValue(CnosDBConstant expectedValue) {
                // TODO: actual converts to double precision
                return expectedValue;
            }

        },
        UNARY_MINUS("-", CnosDBDataType.INT) {
            @Override
            public CnosDBDataType getExpressionType() {
                return CnosDBDataType.INT;
            }

            @Override
            protected CnosDBConstant getExpectedValue(CnosDBConstant expectedValue) {
                if (expectedValue.isNull()) {
                    // TODO
                    throw new IgnoreMeException();
                }
                if (expectedValue.isInt() && expectedValue.asInt() == Long.MIN_VALUE) {
                    throw new IgnoreMeException();
                }
                try {
                    return CnosDBConstant.createIntConstant(-expectedValue.asInt());
                } catch (UnsupportedOperationException e) {
                    return null;
                }
            }

        };

        private final String textRepresentation;
        private final CnosDBDataType[] dataTypes;

        PrefixOperator(String textRepresentation, CnosDBDataType... dataTypes) {
            this.textRepresentation = textRepresentation;
            this.dataTypes = dataTypes.clone();
        }

        public abstract CnosDBDataType getExpressionType();

        protected abstract CnosDBConstant getExpectedValue(CnosDBConstant expectedValue);

        @Override
        public String getTextRepresentation() {
            return toString();
        }

    }

}
