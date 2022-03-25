package sqlancer.mariadb.ast;

import sqlancer.Randomly;

public class MariaDBBinaryOperator extends MariaDBExpression {

    private MariaDBExpression left;
    private MariaDBExpression right;
    private MariaDBBinaryComparisonOperator operator;

    public enum MariaDBBinaryComparisonOperator {
        NOT_EQUAL("!="), LESS_THAN("<"), /* NULL_SAFE_EQUAL("<=>"), EQUALS("="), */ GREATER_THAN(">"),
        GREATER_THAN_EQUAL(">="),

        // regex
        LIKE("LIKE"), RLIKE("RLIKE"), REGEXP("REGEXP"),
        // PLUS("+");
        AND("AND"), OR("OR"), XOR("XOR"),

        BITWISE_AND("&"), LEFT_SHIFT("<<"), RIGHT_SHIFT(">>"), BITWISE_XOR("^"), BITWISE_OR("|");

        private String operator;

        MariaDBBinaryComparisonOperator(String operator) {
            this.operator = operator;
        }

        public String getTextRepresentation() {
            return operator;
        }

        public static MariaDBBinaryComparisonOperator getRandom() {
            return Randomly.fromOptions(MariaDBBinaryComparisonOperator.values());
        }

    };

    public MariaDBBinaryOperator(MariaDBExpression left, MariaDBExpression right, MariaDBBinaryComparisonOperator operator) {
        this.left = left;
        this.right = right;
        this.operator = operator;
    }

    public MariaDBExpression getLeft() {
        return left;
    }

    public MariaDBExpression getRight() {
        return right;
    };

    public MariaDBBinaryComparisonOperator getOp() {
        return operator;
    }

}
