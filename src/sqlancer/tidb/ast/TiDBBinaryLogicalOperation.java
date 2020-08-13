package sqlancer.tidb.ast;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.tidb.ast.TiDBBinaryLogicalOperation.TiDBBinaryLogicalOperator;

public class TiDBBinaryLogicalOperation extends BinaryOperatorNode<TiDBExpression, TiDBBinaryLogicalOperator>
        implements TiDBExpression {

    public enum TiDBBinaryLogicalOperator implements Operator {
        AND("AND"), //
        OR("OR"); //

        String textRepresentation;

        TiDBBinaryLogicalOperator(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        public static TiDBBinaryLogicalOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return textRepresentation;
        }
    }

    public TiDBBinaryLogicalOperation(TiDBExpression left, TiDBExpression right, TiDBBinaryLogicalOperator op) {
        super(left, right, op);
    }

}
