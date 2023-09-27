package sqlancer.cnosdb.ast;

import sqlancer.Randomly;
import sqlancer.cnosdb.CnosDBSchema.CnosDBDataType;
import sqlancer.common.ast.BinaryOperatorNode.Operator;

public class CnosDBPostfixOperation implements CnosDBExpression {

    private final CnosDBExpression expr;
    private final String operatorTextRepresentation;

    public CnosDBPostfixOperation(CnosDBExpression expr, PostfixOperator op) {
        this.expr = expr;
        this.operatorTextRepresentation = Randomly.fromOptions(op.textRepresentations);
    }

    public static CnosDBExpression create(CnosDBExpression expr, PostfixOperator op) {
        return new CnosDBPostfixOperation(expr, op);
    }

    @Override
    public CnosDBDataType getExpressionType() {
        return CnosDBDataType.BOOLEAN;
    }

    public String getOperatorTextRepresentation() {
        return operatorTextRepresentation;
    }

    public CnosDBExpression getExpression() {
        return expr;
    }

    public enum PostfixOperator implements Operator {
        IS_NULL("IS NULL"/* , "ISNULL" */) {
            @Override
            public CnosDBDataType[] getInputDataTypes() {
                return CnosDBDataType.values();
            }

        },
        IS_UNKNOWN("IS UNKNOWN") {
            @Override
            public CnosDBDataType[] getInputDataTypes() {
                return new CnosDBDataType[] { CnosDBDataType.BOOLEAN };
            }
        },

        IS_NOT_NULL("IS NOT NULL"/* "NOTNULL" */) {

            @Override
            public CnosDBDataType[] getInputDataTypes() {
                return CnosDBDataType.values();
            }

        },
        IS_NOT_UNKNOWN("IS NOT UNKNOWN") {

            @Override
            public CnosDBDataType[] getInputDataTypes() {
                return new CnosDBDataType[] { CnosDBDataType.BOOLEAN };
            }
        },
        IS_TRUE("IS TRUE") {
            @Override
            public CnosDBDataType[] getInputDataTypes() {
                return new CnosDBDataType[] { CnosDBDataType.BOOLEAN };
            }

        },
        IS_FALSE("IS FALSE") {
            @Override
            public CnosDBDataType[] getInputDataTypes() {
                return new CnosDBDataType[] { CnosDBDataType.BOOLEAN };
            }

        };

        private final String[] textRepresentations;

        PostfixOperator(String... textRepresentations) {
            this.textRepresentations = textRepresentations.clone();
        }

        public static PostfixOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        public abstract CnosDBDataType[] getInputDataTypes();

        @Override
        public String getTextRepresentation() {
            return toString();
        }
    }

}
