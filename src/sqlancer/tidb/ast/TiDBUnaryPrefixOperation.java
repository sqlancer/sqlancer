package sqlancer.tidb.ast;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.common.ast.UnaryOperatorNode;
import sqlancer.tidb.ast.TiDBUnaryPrefixOperation.TiDBUnaryPrefixOperator;

public class TiDBUnaryPrefixOperation extends UnaryOperatorNode<TiDBExpression, TiDBUnaryPrefixOperator>
        implements TiDBExpression {

    public enum TiDBUnaryPrefixOperator implements Operator {
        NOT("NOT"), //
        INVERSION("~"), //
        PLUS("+"), //
        MINUS("-"), //
        BINARY("BINARY"); //

        private String s;

        TiDBUnaryPrefixOperator(String s) {
            this.s = s;
        }

        public static TiDBUnaryPrefixOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return s;
        }
    }

    public TiDBUnaryPrefixOperation(TiDBExpression expr, TiDBUnaryPrefixOperator op) {
        super(expr, op);
    }

    @Override
    public OperatorKind getOperatorKind() {
        return OperatorKind.PREFIX;
    }

}
