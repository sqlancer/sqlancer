package sqlancer.cockroachdb.ast;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryNode;

public class CockroachDBRegexOperation extends BinaryNode<CockroachDBExpression> implements CockroachDBExpression {

    public enum CockroachDBRegexOperator {
        LIKE("LIKE"), //
        NOT_LIKE("NOT LIKE"), //
        ILIKE("ILIKE"), NOT_ILIKE("NOT ILIKE"), SIMILAR("SIMILAR TO"), //
        NOT_SIMILAR("NOT SIMILAR TO"), MATCH_CASE_SENSITIVE("~"), //
        NOT_MATCH_CASE_SENSITIVE("!~"), //
        MATCH_CASE_INSENSITIVE("~*"), //
        NOT_MATCH_CASE_INSENSITIVE("!~*");

        private String textRepr;

        CockroachDBRegexOperator(String textRepr) {
            this.textRepr = textRepr;
        }

        public static CockroachDBRegexOperator getRandom() {
            return Randomly.fromOptions(values());
        }

    }

    private final CockroachDBRegexOperator op;

    public CockroachDBRegexOperation(CockroachDBExpression left, CockroachDBExpression right,
            CockroachDBRegexOperator op) {
        super(left, right);
        this.op = op;
    }

    @Override
    public String getOperatorRepresentation() {
        return op.textRepr;
    }

}
