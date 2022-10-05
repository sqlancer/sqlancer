package sqlancer.tidb.ast;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.tidb.ast.TiDBRegexOperation.TiDBRegexOperator;

public class TiDBRegexOperation extends BinaryOperatorNode<TiDBExpression, TiDBRegexOperator>
        implements TiDBExpression {

    public enum TiDBRegexOperator implements Operator {
        LIKE("LIKE"), //
        NOT_LIKE("NOT LIKE"), //
        ILIKE("REGEXP"), //
        NOT_REGEXP("NOT REGEXP");

        private String textRepr;

        TiDBRegexOperator(String textRepr) {
            this.textRepr = textRepr;
        }

        public static TiDBRegexOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return textRepr;
        }

    }

    public TiDBRegexOperation(TiDBExpression left, TiDBExpression right, TiDBRegexOperator op) {
        super(left, right, op);
    }

}
