package sqlancer.cnosdb.ast;

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

        },
        UNARY_PLUS("+", CnosDBDataType.INT) {
            @Override
            public CnosDBDataType getExpressionType() {
                return CnosDBDataType.INT;
            }

        },
        UNARY_MINUS("-", CnosDBDataType.INT) {
            @Override
            public CnosDBDataType getExpressionType() {
                return CnosDBDataType.INT;
            }

        };

        private final String textRepresentation;
        private final CnosDBDataType[] dataTypes;

        PrefixOperator(String textRepresentation, CnosDBDataType... dataTypes) {
            this.textRepresentation = textRepresentation;
            this.dataTypes = dataTypes.clone();
        }

        public abstract CnosDBDataType getExpressionType();

        @Override
        public String getTextRepresentation() {
            return toString();
        }

    }

}
