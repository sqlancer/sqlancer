package sqlancer.cockroachdb.ast;

import sqlancer.Randomly;
import sqlancer.cockroachdb.ast.CockroachDBBinaryArithmeticOperation.CockroachDBBinaryArithmeticOperator;
import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.BinaryOperatorNode.Operator;

public class CockroachDBBinaryArithmeticOperation
        extends BinaryOperatorNode<CockroachDBExpression, CockroachDBBinaryArithmeticOperator>
        implements CockroachDBExpression {

    public enum CockroachDBBinaryArithmeticOperator implements Operator {
        ADD("+"), MULT("*"), MINUS("-"), DIV("/");

        String textRepresentation;

        CockroachDBBinaryArithmeticOperator(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        public static CockroachDBBinaryArithmeticOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return textRepresentation;
        }

    }

    public CockroachDBBinaryArithmeticOperation(CockroachDBExpression left, CockroachDBExpression right,
            CockroachDBBinaryArithmeticOperator op) {
        super(left, right, op);
    }

}
