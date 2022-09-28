package sqlancer.databend.ast;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.newast.NewBinaryOperatorNode;
import sqlancer.common.ast.newast.Node;

public class DatabendLikeOperation extends NewBinaryOperatorNode<DatabendExpression> {

    public DatabendLikeOperation(Node<DatabendExpression> left, Node<DatabendExpression> right,
            DatabendLikeOperator op) {
        super(left, right, op);
    }

    public enum DatabendLikeOperator implements BinaryOperatorNode.Operator {
        LIKE_OPERATOR("LIKE", "like");

        private final String[] textRepresentations;

        DatabendLikeOperator(String... text) {
            textRepresentations = text.clone();
        }

        @Override
        public String getTextRepresentation() {
            return Randomly.fromOptions(textRepresentations);
        }
    }

}
