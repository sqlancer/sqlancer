package sqlancer.voltdb.ast;

import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.common.ast.newast.NewBinaryOperatorNode;

public class VoltDBBinaryOperation extends NewBinaryOperatorNode<VoltDBExpression> implements VoltDBExpression {

    public enum VoltDBBinaryOperator implements Operator {
        AND(" AND "), OR(" OR "), //
        EQUALS(" = "), NOT_EQUALS(" <> "), SMALLER(" < "), SMALLER_EQUALS(" <= "), GREATER(" > "),
        GREATER_EQUALS(" >= "), //
        LIKE(" LIKE "), NOT_LIKE(" NOT LIKE "), //
        ADD(" + "), SUBTRACT(" - "), MULTIPLY(" * "), DIVIDE(" / "), //
        CONCAT(" || ");

        private final String text;

        VoltDBBinaryOperator(String text) {
            this.text = text;
        }

        @Override
        public String getTextRepresentation() {
            return text;
        }

        public boolean isOrdering() {
            return this == SMALLER || this == SMALLER_EQUALS || this == GREATER || this == GREATER_EQUALS;
        }
    }

    public VoltDBBinaryOperation(VoltDBExpression left, VoltDBExpression right, VoltDBBinaryOperator op) {
        super(left, right, op);
    }

}
