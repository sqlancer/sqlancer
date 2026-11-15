package sqlancer.voltdb.ast;

import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.common.ast.newast.NewUnaryPostfixOperatorNode;

public class VoltDBUnaryPostfixOperation extends NewUnaryPostfixOperatorNode<VoltDBExpression>
        implements VoltDBExpression {

    public enum VoltDBUnaryPostfixOperator implements Operator {
        IS_NULL("IS NULL"), IS_NOT_NULL("IS NOT NULL");

        private final String text;

        VoltDBUnaryPostfixOperator(String text) {
            this.text = text;
        }

        @Override
        public String getTextRepresentation() {
            return text;
        }
    }

    public VoltDBUnaryPostfixOperation(VoltDBExpression expr, VoltDBUnaryPostfixOperator op) {
        super(expr, op);
    }

}
