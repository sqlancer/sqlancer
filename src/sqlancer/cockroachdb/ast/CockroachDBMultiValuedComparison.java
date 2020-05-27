package sqlancer.cockroachdb.ast;

import java.util.List;

import sqlancer.Randomly;

public class CockroachDBMultiValuedComparison implements CockroachDBExpression {

    private CockroachDBExpression left;
    private List<CockroachDBExpression> right;
    private MultiValuedComparisonType type;
    private MultiValuedComparisonOperator op;

    public enum MultiValuedComparisonType {
        ANY, SOME, ALL;

        public static MultiValuedComparisonType getRandom() {
            return Randomly.fromOptions(values());
        }
    }

    public enum MultiValuedComparisonOperator {
        SMALLER("<"), GREATER(">"), EQUALS("="), SMALLER_EQUALS("<="), GREATER_EQUALS(">="), NOT_EQUALS("!=");

        private String stringRepr;

        MultiValuedComparisonOperator(String stringRepr) {
            this.stringRepr = stringRepr;
        }

        public String getStringRepr() {
            return stringRepr;
        }

        public static MultiValuedComparisonOperator getRandomGenericComparisonOperator() {
            return Randomly.fromOptions(SMALLER, GREATER);
        }
    }

    public CockroachDBMultiValuedComparison(CockroachDBExpression left, List<CockroachDBExpression> right,
            MultiValuedComparisonType type, MultiValuedComparisonOperator op) {
        this.left = left;
        this.right = right;
        this.type = type;
        this.op = op;
    }

    public CockroachDBExpression getLeft() {
        return left;
    }

    public MultiValuedComparisonOperator getOp() {
        return op;
    }

    public List<CockroachDBExpression> getRight() {
        return right;
    }

    public MultiValuedComparisonType getType() {
        return type;
    }

}
