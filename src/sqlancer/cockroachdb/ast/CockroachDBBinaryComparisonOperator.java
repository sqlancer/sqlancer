package sqlancer.cockroachdb.ast;

import sqlancer.Randomly;
import sqlancer.cockroachdb.ast.CockroachDBBinaryComparisonOperator.CockroachDBComparisonOperator;
import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.BinaryOperatorNode.Operator;

public class CockroachDBBinaryComparisonOperator extends
        BinaryOperatorNode<CockroachDBExpression, CockroachDBComparisonOperator> implements CockroachDBExpression {

    public enum CockroachDBComparisonOperator implements Operator {
        EQUALS("="), GREATER(">"), GREATER_EQUALS(">="), SMALLER("<"), SMALLER_EQUALS("<="), NOT_EQUALS("!="),
        IS_DISTINCT_FROM("IS DISTINCT FROM"), IS_NOT_DISTINCT_FROM("IS NOT DISTINCT FROM");

        private String textRepr;

        CockroachDBComparisonOperator(String textRepr) {
            this.textRepr = textRepr;
        }

        public static CockroachDBComparisonOperator getRandom() {
            return Randomly.fromOptions(CockroachDBComparisonOperator.values());
        }

        @Override
        public String getTextRepresentation() {
            return textRepr;
        }

    }

    public CockroachDBBinaryComparisonOperator(CockroachDBExpression left, CockroachDBExpression right,
            CockroachDBComparisonOperator op) {
        super(left, right, op);
    }

}
