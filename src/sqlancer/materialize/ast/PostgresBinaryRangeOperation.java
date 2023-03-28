package sqlancer.postgres.ast;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryNode;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.postgres.PostgresSchema.PostgresDataType;

public class PostgresBinaryRangeOperation extends BinaryNode<PostgresExpression> implements PostgresExpression {

    private final String op;

    public enum PostgresBinaryRangeOperator implements Operator {
        UNION("+"), INTERSECTION("*"), DIFFERENCE("-");

        private final String textRepresentation;

        PostgresBinaryRangeOperator(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        @Override
        public String getTextRepresentation() {
            return textRepresentation;
        }

        public static PostgresBinaryRangeOperator getRandom() {
            return Randomly.fromOptions(values());
        }

    }

    public enum PostgresBinaryRangeComparisonOperator {
        CONTAINS_RANGE_OR_ELEMENT("@>"), RANGE_OR_ELEMENT_IS_CONTAINED("<@"), OVERLAP("&&"), STRICT_LEFT_OF("<<"),
        STRICT_RIGHT_OF(">>"), NOT_RIGHT_OF("&<"), NOT_LEFT_OF(">&"), ADJACENT("-|-");

        private final String textRepresentation;

        PostgresBinaryRangeComparisonOperator(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        public String getTextRepresentation() {
            return textRepresentation;
        }

        public static PostgresBinaryRangeComparisonOperator getRandom() {
            return Randomly.fromOptions(values());
        }
    }

    public PostgresBinaryRangeOperation(PostgresBinaryRangeComparisonOperator op, PostgresExpression left,
            PostgresExpression right) {
        super(left, right);
        this.op = op.getTextRepresentation();
    }

    public PostgresBinaryRangeOperation(PostgresBinaryRangeOperator op, PostgresExpression left,
            PostgresExpression right) {
        super(left, right);
        this.op = op.getTextRepresentation();
    }

    @Override
    public PostgresDataType getExpressionType() {
        return PostgresDataType.BOOLEAN;
    }

    @Override
    public String getOperatorRepresentation() {
        return op;
    }

}
