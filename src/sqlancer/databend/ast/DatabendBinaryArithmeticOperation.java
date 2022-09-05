package sqlancer.databend.ast;

import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.newast.NewBinaryOperatorNode;
import sqlancer.common.ast.newast.Node;

public class DatabendBinaryArithmeticOperation extends NewBinaryOperatorNode<DatabendExpression> {

    public DatabendBinaryArithmeticOperation(Node<DatabendExpression> left, Node<DatabendExpression> right, BinaryOperatorNode.Operator op) {
        super(left, right, op);
    }

    public enum DatabendBinaryArithmeticOperator implements BinaryOperatorNode.Operator{
        ADDITION("+"),
        SUBTRACTION("-"),
        MULTIPLICATION("*"),
        DIVISION("/"),
        MODULO("%");

        DatabendBinaryArithmeticOperator(String text) {
            textRepresentation = text;
        }
        private String textRepresentation;

        @Override
        public String getTextRepresentation() {
            return textRepresentation;
        }
    }

}
