package sqlancer.cnosdb.ast;

import sqlancer.Randomly;
import sqlancer.cnosdb.CnosDBSchema.CnosDBDataType;
import sqlancer.common.ast.BinaryOperatorNode.Operator;

public class CnosDBPostfixOperation implements CnosDBExpression {

    private final CnosDBExpression expr;
    private final PostfixOperator op;
    private final String operatorTextRepresentation;

    public enum PostfixOperator implements Operator {
        IS_NULL("IS NULL"/* , "ISNULL" */) {
            @Override
            public CnosDBConstant apply(CnosDBConstant expectedValue) {
                return CnosDBConstant.createBooleanConstant(expectedValue.isNull());
            }

            @Override
            public CnosDBDataType[] getInputDataTypes() {
                return CnosDBDataType.values();
            }

        },
        IS_UNKNOWN("IS UNKNOWN") {
            @Override
            public CnosDBConstant apply(CnosDBConstant expectedValue) {
                return CnosDBConstant.createBooleanConstant(expectedValue.isNull());
            }

            @Override
            public CnosDBDataType[] getInputDataTypes() {
                return new CnosDBDataType[] { CnosDBDataType.BOOLEAN };
            }
        },

        IS_NOT_NULL("IS NOT NULL"/* "NOTNULL" */) {
            @Override
            public CnosDBConstant apply(CnosDBConstant expectedValue) {
                return CnosDBConstant.createBooleanConstant(!expectedValue.isNull());
            }

            @Override
            public CnosDBDataType[] getInputDataTypes() {
                return CnosDBDataType.values();
            }

        },
        IS_NOT_UNKNOWN("IS NOT UNKNOWN") {
            @Override
            public CnosDBConstant apply(CnosDBConstant expectedValue) {
                return CnosDBConstant.createBooleanConstant(!expectedValue.isNull());
            }

            @Override
            public CnosDBDataType[] getInputDataTypes() {
                return new CnosDBDataType[] { CnosDBDataType.BOOLEAN };
            }
        },
        IS_TRUE("IS TRUE") {
            @Override
            public CnosDBConstant apply(CnosDBConstant expectedValue) {
                if (expectedValue.isNull()) {
                    return CnosDBConstant.createFalse();
                } else {
                    return CnosDBConstant.createBooleanConstant(expectedValue.cast(CnosDBDataType.BOOLEAN).asBoolean());
                }
            }

            @Override
            public CnosDBDataType[] getInputDataTypes() {
                return new CnosDBDataType[] { CnosDBDataType.BOOLEAN };
            }

        },
        IS_FALSE("IS FALSE") {
            @Override
            public CnosDBConstant apply(CnosDBConstant expectedValue) {
                if (expectedValue.isNull()) {
                    return CnosDBConstant.createFalse();
                } else {
                    return CnosDBConstant
                            .createBooleanConstant(!expectedValue.cast(CnosDBDataType.BOOLEAN).asBoolean());
                }
            }

            @Override
            public CnosDBDataType[] getInputDataTypes() {
                return new CnosDBDataType[] { CnosDBDataType.BOOLEAN };
            }

        };

        private String[] textRepresentations;

        PostfixOperator(String... textRepresentations) {
            this.textRepresentations = textRepresentations.clone();
        }

        public abstract CnosDBConstant apply(CnosDBConstant expectedValue);

        public abstract CnosDBDataType[] getInputDataTypes();

        public static PostfixOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return toString();
        }
    }

    public CnosDBPostfixOperation(CnosDBExpression expr, PostfixOperator op) {
        this.expr = expr;
        this.operatorTextRepresentation = Randomly.fromOptions(op.textRepresentations);
        this.op = op;
    }

    @Override
    public CnosDBDataType getExpressionType() {
        return CnosDBDataType.BOOLEAN;
    }

    @Override
    public CnosDBConstant getExpectedValue() {
        CnosDBConstant expectedValue = expr.getExpectedValue();
        if (expectedValue == null) {
            return null;
        }
        return op.apply(expectedValue);
    }

    public String getOperatorTextRepresentation() {
        return operatorTextRepresentation;
    }

    public static CnosDBExpression create(CnosDBExpression expr, PostfixOperator op) {
        return new CnosDBPostfixOperation(expr, op);
    }

    public CnosDBExpression getExpression() {
        return expr;
    }

}
