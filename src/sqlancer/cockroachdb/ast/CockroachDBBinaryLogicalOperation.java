package sqlancer.cockroachdb.ast;

import sqlancer.Randomly;
import sqlancer.cockroachdb.ast.CockroachDBBinaryLogicalOperation.CockroachDBBinaryLogicalOperator;
import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.BinaryOperatorNode.Operator;

public class CockroachDBBinaryLogicalOperation extends
        BinaryOperatorNode<CockroachDBExpression, CockroachDBBinaryLogicalOperator> implements CockroachDBExpression {

    public enum CockroachDBBinaryLogicalOperator implements Operator {
        AND("AND"), OR("OR");

        private String textRepr;

        CockroachDBBinaryLogicalOperator(String textRepr) {
            this.textRepr = textRepr;
        }

        public static CockroachDBBinaryLogicalOperator getRandom() {
            return Randomly.fromOptions(CockroachDBBinaryLogicalOperator.values());
        }

        @Override
        public String getTextRepresentation() {
            return textRepr;
        }

    }

    public CockroachDBBinaryLogicalOperation(CockroachDBExpression left, CockroachDBExpression right,
            CockroachDBBinaryLogicalOperator op) {
        super(left, right, op);
    }

}
