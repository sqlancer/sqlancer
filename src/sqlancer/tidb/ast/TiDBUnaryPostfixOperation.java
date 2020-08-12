package sqlancer.tidb.ast;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.common.ast.UnaryOperatorNode;
import sqlancer.tidb.ast.TiDBUnaryPostfixOperation.TiDBUnaryPostfixOperator;

public class TiDBUnaryPostfixOperation extends UnaryOperatorNode<TiDBExpression, TiDBUnaryPostfixOperator>
        implements TiDBExpression {

    public enum TiDBUnaryPostfixOperator implements Operator {
        IS_NULL("IS NULL"), //
        IS_NOT_NULL("IS NOT NULL"); //

        private String s;

        TiDBUnaryPostfixOperator(String s) {
            this.s = s;
        }

        public static TiDBUnaryPostfixOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return s;
        }
    }

    public TiDBUnaryPostfixOperation(TiDBExpression expr, TiDBUnaryPostfixOperator op) {
        super(expr, op);
    }

    @Override
    public OperatorKind getOperatorKind() {
        return OperatorKind.POSTFIX;
    }

}
