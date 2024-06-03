package sqlancer.cnosdb.ast;

import sqlancer.Randomly;
import sqlancer.cnosdb.CnosDBSchema.CnosDBDataType;
import sqlancer.common.ast.BinaryOperatorNode.Operator;

public class CnosDBPostfixOperation implements CnosDBExpression {

    private final CnosDBExpression expr;
    private final Operator op;

    public CnosDBPostfixOperation(CnosDBExpression expr, PostfixOperator op) {
        this.expr = expr;
        this.op = op;
    }

    @Override
    public CnosDBDataType getExpressionType() {
        return CnosDBDataType.BOOLEAN;
    }

    public PostfixOperator getOp() {
        return (PostfixOperator) op;
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
        IS_NOT_NULL("IS NOT NULL"/* "NOTNULL" */) {

            @Override
            public CnosDBDataType[] getInputDataTypes() {
                return CnosDBDataType.values();
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

        private final String textRepresentations;

        PostfixOperator(String text) {
            this.textRepresentations = text;
        }

        public static PostfixOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        public abstract CnosDBDataType[] getInputDataTypes();

        @Override
        public String getTextRepresentation() {
            return textRepresentations;
        }
    }

    public String getOperatorRepresentation(){
        return op.getTextRepresentation();
    }

}
