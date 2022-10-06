package sqlancer.yugabyte.ysql.ast;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryNode;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLDataType;

public class YSQLBinaryRangeOperation extends BinaryNode<YSQLExpression> implements YSQLExpression {

    private final String op;

    public YSQLBinaryRangeOperation(YSQLBinaryRangeComparisonOperator op, YSQLExpression left, YSQLExpression right) {
        super(left, right);
        this.op = op.getTextRepresentation();
    }

    public YSQLBinaryRangeOperation(YSQLBinaryRangeOperator op, YSQLExpression left, YSQLExpression right) {
        super(left, right);
        this.op = op.getTextRepresentation();
    }

    @Override
    public YSQLDataType getExpressionType() {
        return YSQLDataType.BOOLEAN;
    }

    @Override
    public String getOperatorRepresentation() {
        return op;
    }

    public enum YSQLBinaryRangeOperator implements Operator {
        UNION("+"), INTERSECTION("*"), DIFFERENCE("-");

        private final String textRepresentation;

        YSQLBinaryRangeOperator(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        public static YSQLBinaryRangeOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return textRepresentation;
        }

    }

    public enum YSQLBinaryRangeComparisonOperator {
        CONTAINS_RANGE_OR_ELEMENT("@>"), RANGE_OR_ELEMENT_IS_CONTAINED("<@"), OVERLAP("&&"), STRICT_LEFT_OF("<<"),
        STRICT_RIGHT_OF(">>"), NOT_RIGHT_OF("&<"), NOT_LEFT_OF(">&"), ADJACENT("-|-");

        private final String textRepresentation;

        YSQLBinaryRangeComparisonOperator(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        public static YSQLBinaryRangeComparisonOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        public String getTextRepresentation() {
            return textRepresentation;
        }
    }

}
