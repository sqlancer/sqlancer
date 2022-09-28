package sqlancer.databend.ast;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.newast.NewBinaryOperatorNode;
import sqlancer.common.ast.newast.Node;

public class DatabendBinaryLogicalOperation extends NewBinaryOperatorNode<DatabendExpression> {

    public DatabendBinaryLogicalOperation(Node<DatabendExpression> left, Node<DatabendExpression> right,
            DatabendBinaryLogicalOperator op) {
        super(left, right, op);
    }

    public enum DatabendBinaryLogicalOperator implements BinaryOperatorNode.Operator {
        AND("AND", "and"), OR("OR", "or");

        private final String[] textRepresentations;

        DatabendBinaryLogicalOperator(String... textRepresentations) {
            this.textRepresentations = textRepresentations.clone();
        }

        @Override
        public String getTextRepresentation() {
            return Randomly.fromOptions(textRepresentations);
        }

        public DatabendBinaryLogicalOperator getRandomOp() {
            return Randomly.fromOptions(values());
        }

        public static DatabendBinaryLogicalOperator getRandom() {
            return Randomly.fromOptions(values());
        }
    }

}
