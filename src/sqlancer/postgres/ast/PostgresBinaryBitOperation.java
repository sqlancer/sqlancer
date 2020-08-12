package sqlancer.postgres.ast;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.postgres.PostgresSchema.PostgresDataType;
import sqlancer.postgres.ast.PostgresBinaryBitOperation.PostgresBinaryBitOperator;

public class PostgresBinaryBitOperation extends BinaryOperatorNode<PostgresExpression, PostgresBinaryBitOperator>
        implements PostgresExpression {

    public enum PostgresBinaryBitOperator implements Operator {
        CONCATENATION("||"), //
        BITWISE_AND("&"), //
        BITWISE_OR("|"), //
        BITWISE_XOR("#"), //
        BITWISE_SHIFT_LEFT("<<"), //
        BITWISE_SHIFT_RIGHT(">>");

        private String text;

        PostgresBinaryBitOperator(String text) {
            this.text = text;
        }

        public static PostgresBinaryBitOperator getRandom() {
            return Randomly.fromOptions(PostgresBinaryBitOperator.values());
        }

        @Override
        public String getTextRepresentation() {
            return text;
        }

    }

    public PostgresBinaryBitOperation(PostgresBinaryBitOperator op, PostgresExpression left, PostgresExpression right) {
        super(left, right, op);
    }

    @Override
    public PostgresDataType getExpressionType() {
        return PostgresDataType.BIT;
    }

}
