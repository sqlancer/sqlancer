package sqlancer.postgres.ast;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryNode;
import sqlancer.common.ast.BinaryOperatorNode;

public class PostgresExtractorJsonOperation extends BinaryNode<PostgresExpression> implements PostgresExpression {

    private final String op;

    public enum PostgresExtractorJsonOperator implements BinaryOperatorNode.Operator {
        TEXT_EXTRACTOR("->>"), JSON_EXTRACTOR("->");

        private final String textRepresentation;

        PostgresExtractorJsonOperator(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        @Override
        public String getTextRepresentation() {
            return textRepresentation;
        }

        public static PostgresExtractorJsonOperator getRandom() {
            return Randomly.fromOptions(values());
        }

    }

    public PostgresExtractorJsonOperation(PostgresExtractorJsonOperator op, PostgresExpression left,
            PostgresExpression right) {
        super(left, right);
        this.op = op.getTextRepresentation();
    }

    @Override
    public String getOperatorRepresentation() {
        return this.op;
    }

}
