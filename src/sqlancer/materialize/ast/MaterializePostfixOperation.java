package sqlancer.materialize.ast;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.materialize.MaterializeSchema.MaterializeDataType;

public class MaterializePostfixOperation implements MaterializeExpression {

    private final MaterializeExpression expr;
    private final PostfixOperator op;
    private final String operatorTextRepresentation;

    public enum PostfixOperator implements Operator {
        IS_NULL("IS NULL", "ISNULL") {
            @Override
            public MaterializeConstant apply(MaterializeConstant expectedValue) {
                return MaterializeConstant.createBooleanConstant(expectedValue.isNull());
            }

            @Override
            public MaterializeDataType[] getInputDataTypes() {
                return MaterializeDataType.values();
            }

        },
        IS_UNKNOWN("IS UNKNOWN") {
            @Override
            public MaterializeConstant apply(MaterializeConstant expectedValue) {
                return MaterializeConstant.createBooleanConstant(expectedValue.isNull());
            }

            @Override
            public MaterializeDataType[] getInputDataTypes() {
                return new MaterializeDataType[] { MaterializeDataType.BOOLEAN };
            }
        },

        IS_NOT_NULL("IS NOT NULL") {

            @Override
            public MaterializeConstant apply(MaterializeConstant expectedValue) {
                return MaterializeConstant.createBooleanConstant(!expectedValue.isNull());
            }

            @Override
            public MaterializeDataType[] getInputDataTypes() {
                return MaterializeDataType.values();
            }

        },
        IS_NOT_UNKNOWN("IS NOT UNKNOWN") {
            @Override
            public MaterializeConstant apply(MaterializeConstant expectedValue) {
                return MaterializeConstant.createBooleanConstant(!expectedValue.isNull());
            }

            @Override
            public MaterializeDataType[] getInputDataTypes() {
                return new MaterializeDataType[] { MaterializeDataType.BOOLEAN };
            }
        },
        IS_TRUE("IS TRUE") {

            @Override
            public MaterializeConstant apply(MaterializeConstant expectedValue) {
                if (expectedValue.isNull()) {
                    return MaterializeConstant.createFalse();
                } else {
                    return MaterializeConstant
                            .createBooleanConstant(expectedValue.cast(MaterializeDataType.BOOLEAN).asBoolean());
                }
            }

            @Override
            public MaterializeDataType[] getInputDataTypes() {
                return new MaterializeDataType[] { MaterializeDataType.BOOLEAN };
            }

        },
        IS_FALSE("IS FALSE") {

            @Override
            public MaterializeConstant apply(MaterializeConstant expectedValue) {
                if (expectedValue.isNull()) {
                    return MaterializeConstant.createFalse();
                } else {
                    return MaterializeConstant
                            .createBooleanConstant(!expectedValue.cast(MaterializeDataType.BOOLEAN).asBoolean());
                }
            }

            @Override
            public MaterializeDataType[] getInputDataTypes() {
                return new MaterializeDataType[] { MaterializeDataType.BOOLEAN };
            }

        };

        private String[] textRepresentations;

        PostfixOperator(String... textRepresentations) {
            this.textRepresentations = textRepresentations.clone();
        }

        public abstract MaterializeConstant apply(MaterializeConstant expectedValue);

        public abstract MaterializeDataType[] getInputDataTypes();

        public static PostfixOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return toString();
        }
    }

    public MaterializePostfixOperation(MaterializeExpression expr, PostfixOperator op) {
        this.expr = expr;
        this.operatorTextRepresentation = Randomly.fromOptions(op.textRepresentations);
        this.op = op;
    }

    @Override
    public MaterializeDataType getExpressionType() {
        return MaterializeDataType.BOOLEAN;
    }

    @Override
    public MaterializeConstant getExpectedValue() {
        MaterializeConstant expectedValue = expr.getExpectedValue();
        if (expectedValue == null) {
            return null;
        }
        return op.apply(expectedValue);
    }

    public String getOperatorTextRepresentation() {
        return operatorTextRepresentation;
    }

    public static MaterializeExpression create(MaterializeExpression expr, PostfixOperator op) {
        return new MaterializePostfixOperation(expr, op);
    }

    public MaterializeExpression getExpression() {
        return expr;
    }

}
