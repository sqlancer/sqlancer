package sqlancer.mariadb.ast;

import sqlancer.Randomly;

public class MariaDBPostfixUnaryOperation extends MariaDBExpression {

    private MariaDBPostfixUnaryOperator operator;
    private MariaDBExpression randomWhereCondition;

    public MariaDBPostfixUnaryOperation(MariaDBPostfixUnaryOperator operator, MariaDBExpression randomWhereCondition) {
        this.operator = operator;
        this.randomWhereCondition = randomWhereCondition;
    }

    public enum MariaDBPostfixUnaryOperator {
        IS_TRUE("IS TRUE"), IS_FALSE("IS FALSE"), IS_NULL("IS NULL"), IS_NOT_NULL("IS NOT NULL");

        private final String textRepr;

        MariaDBPostfixUnaryOperator(String textRepr) {
            this.textRepr = textRepr;
        }

        public String getTextRepresentation() {
            return textRepr;
        }

        public static MariaDBPostfixUnaryOperator getRandom() {
            return Randomly.fromOptions(values());
        }
    }

    public MariaDBPostfixUnaryOperator getOperator() {
        return operator;
    }

    public MariaDBExpression getRandomWhereCondition() {
        return randomWhereCondition;
    }
}
