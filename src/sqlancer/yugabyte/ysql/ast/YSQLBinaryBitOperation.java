package sqlancer.yugabyte.ysql.ast;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLDataType;
import sqlancer.yugabyte.ysql.ast.YSQLBinaryBitOperation.YSQLBinaryBitOperator;

public class YSQLBinaryBitOperation extends BinaryOperatorNode<YSQLExpression, YSQLBinaryBitOperator>
        implements YSQLExpression {

    public YSQLBinaryBitOperation(YSQLBinaryBitOperator op, YSQLExpression left, YSQLExpression right) {
        super(left, right, op);
    }

    @Override
    public YSQLDataType getExpressionType() {
        return YSQLDataType.BIT;
    }

    public enum YSQLBinaryBitOperator implements Operator {
        CONCATENATION("||"), //
        BITWISE_AND("&"), //
        BITWISE_OR("|"), //
        BITWISE_XOR("#"), //
        BITWISE_SHIFT_LEFT("<<"), //
        BITWISE_SHIFT_RIGHT(">>");

        private final String text;

        YSQLBinaryBitOperator(String text) {
            this.text = text;
        }

        public static YSQLBinaryBitOperator getRandom() {
            return Randomly.fromOptions(YSQLBinaryBitOperator.values());
        }

        @Override
        public String getTextRepresentation() {
            return text;
        }

    }

}
