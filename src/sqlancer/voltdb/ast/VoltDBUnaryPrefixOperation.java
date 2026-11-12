package sqlancer.voltdb.ast;

import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.common.ast.newast.NewUnaryPrefixOperatorNode;

public class VoltDBUnaryPrefixOperation extends NewUnaryPrefixOperatorNode<VoltDBExpression>
        implements VoltDBExpression {

    public enum VoltDBUnaryPrefixOperator implements Operator {
        NOT("NOT"), MINUS("-");

        private final String text;

        VoltDBUnaryPrefixOperator(String text) {
            this.text = text;
        }

        @Override
        public String getTextRepresentation() {
            return text;
        }
    }

    public VoltDBUnaryPrefixOperation(VoltDBExpression expr, VoltDBUnaryPrefixOperator op) {
        super(expr, op);
    }

}
