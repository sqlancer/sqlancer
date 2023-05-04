package sqlancer.cnosdb.ast;

import sqlancer.Randomly;
import sqlancer.cnosdb.CnosDBSchema.CnosDBDataType;
import sqlancer.cnosdb.ast.CnosDBBinaryComparisonOperation.CnosDBBinaryComparisonOperator;
import sqlancer.common.ast.BinaryOperatorNode;

public class CnosDBBinaryComparisonOperation
        extends BinaryOperatorNode<CnosDBExpression, CnosDBBinaryComparisonOperator> implements CnosDBExpression {

    public CnosDBBinaryComparisonOperation(CnosDBExpression left, CnosDBExpression right,
            CnosDBBinaryComparisonOperator op) {
        super(left, right, op);
    }

    @Override
    public CnosDBDataType getExpressionType() {
        return CnosDBDataType.BOOLEAN;
    }

    public enum CnosDBBinaryComparisonOperator implements BinaryOperatorNode.Operator {
        EQUALS("=") {
        },
        IS_DISTINCT("IS DISTINCT FROM") {
        },
        IS_NOT_DISTINCT("IS NOT DISTINCT FROM") {
        },
        NOT_EQUALS("!=") {
        },
        LESS("<") {
        },
        LESS_EQUALS("<=") {
        },
        GREATER(">") {
        },
        GREATER_EQUALS(">=") {

        };

        private final String textRepresentation;

        CnosDBBinaryComparisonOperator(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        public static CnosDBBinaryComparisonOperator getRandom() {
            return Randomly.fromOptions(CnosDBBinaryComparisonOperator.values());
        }

        @Override
        public String getTextRepresentation() {
            return textRepresentation;
        }

    }

}
