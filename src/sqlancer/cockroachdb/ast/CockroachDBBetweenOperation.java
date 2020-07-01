package sqlancer.cockroachdb.ast;

import sqlancer.Randomly;

public class CockroachDBBetweenOperation implements CockroachDBExpression {

    private final CockroachDBExpression expr;
    private final CockroachDBExpression left;
    private final CockroachDBExpression right;
    private CockroachDBBetweenOperatorType type;

    public enum CockroachDBBetweenOperatorType {
        BETWEEN("BETWEEN"), NOT_BETWEEN("NOT BETWEEN"), BETWEEN_SYMMETRIC("BETWEEN SYMMETRIC"),
        NOT_BETWEEN_SYMMETRIC("NOT BETWEEN SYMMETRIC");

        private String s;

        CockroachDBBetweenOperatorType(String s) {
            this.s = s;
        }

        public String getStringRepresentation() {
            return s;
        }

        public static CockroachDBBetweenOperatorType getRandom() {
            return Randomly.fromOptions(values());
        }
    };

    public CockroachDBBetweenOperation(CockroachDBExpression expr, CockroachDBExpression left,
            CockroachDBExpression right, CockroachDBBetweenOperatorType type) {
        this.expr = expr;
        this.left = left;
        this.right = right;
        this.type = type;
    }

    public CockroachDBExpression getLeft() {
        return left;
    }

    public CockroachDBExpression getRight() {
        return right;
    }

    public CockroachDBExpression getExpr() {
        return expr;
    };

    public CockroachDBBetweenOperatorType getType() {
        return type;
    }

}
