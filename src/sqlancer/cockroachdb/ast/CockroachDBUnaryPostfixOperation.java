package sqlancer.cockroachdb.ast;

import sqlancer.cockroachdb.ast.CockroachDBUnaryPostfixOperation.CockroachDBUnaryPostfixOperator;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.common.ast.UnaryOperatorNode;

public class CockroachDBUnaryPostfixOperation extends
        UnaryOperatorNode<CockroachDBExpression, CockroachDBUnaryPostfixOperator> implements CockroachDBExpression {

    public enum CockroachDBUnaryPostfixOperator implements Operator {
        IS_NULL("IS NULL"), //
        IS_NOT_NULL("IS NOT NULL"), //
        IS_NAN("IS NAN"), //
        IS_NOT_NAN("IS NOT NAN"), IS_TRUE("IS TRUE"), IS_FALSE("IS FALSE"), IS_NOT_TRUE("IS NOT TRUE"),
        IS_NOT_FALSE("IS NOT FALSE");

        private String s;

        CockroachDBUnaryPostfixOperator(String s) {
            this.s = s;
        }

        @Override
        public String getTextRepresentation() {
            return s;
        }
    }

    public CockroachDBUnaryPostfixOperation(CockroachDBExpression expr, CockroachDBUnaryPostfixOperator op) {
        super(expr, op);
    }

    @Override
    public OperatorKind getOperatorKind() {
        return OperatorKind.POSTFIX;
    }

}
