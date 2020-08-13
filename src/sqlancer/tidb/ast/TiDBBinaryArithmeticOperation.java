package sqlancer.tidb.ast;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.tidb.ast.TiDBBinaryArithmeticOperation.TiDBBinaryArithmeticOperator;

public class TiDBBinaryArithmeticOperation extends BinaryOperatorNode<TiDBExpression, TiDBBinaryArithmeticOperator>
        implements TiDBExpression {

    public enum TiDBBinaryArithmeticOperator implements Operator {
        ADD("+"), //
        MINUS("-"), //
        MULT("*"), //
        DIV("/"), //
        INTEGER_DIV("DIV"), //
        MOD("%"); //

        String textRepresentation;

        TiDBBinaryArithmeticOperator(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        public static TiDBBinaryArithmeticOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return textRepresentation;
        }
    }

    public TiDBBinaryArithmeticOperation(TiDBExpression left, TiDBExpression right, TiDBBinaryArithmeticOperator op) {
        super(left, right, op);
    }

}
