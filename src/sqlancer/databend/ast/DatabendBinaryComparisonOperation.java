package sqlancer.databend.ast;

import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.newast.NewBinaryOperatorNode;
import sqlancer.common.ast.newast.Node;


public class DatabendBinaryComparisonOperation extends NewBinaryOperatorNode<DatabendExpression> {

    public DatabendBinaryComparisonOperation(Node<DatabendExpression> left, Node<DatabendExpression> right,
                                             DatabendBinaryComparisonOperator op) {
        super(left, right, op);
    }

    public enum DatabendBinaryComparisonOperator implements BinaryOperatorNode.Operator{
        EQUALS("="),
//        IS_DISTINCT("IS DISTINCT FROM"),
//        IS_NOT_DISTINCT("IS NOT DISTINCT FROM"),
        NOT_EQUALS("!="),
        LESS("<"),
        LESS_EQUALS("<="),
        GREATER(">"),
        GREATER_EQUALS(">=");

        DatabendBinaryComparisonOperator(String text) {
            textRepresentation = text;
        }
        private String textRepresentation;

        @Override
        public String getTextRepresentation() {
            return textRepresentation;
        }
    }


}
