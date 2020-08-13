package sqlancer.cockroachdb.ast;

import sqlancer.common.ast.UnaryNode;

public class CockroachDBNotOperation extends UnaryNode<CockroachDBExpression> implements CockroachDBExpression {

    public CockroachDBNotOperation(CockroachDBExpression expr) {
        super(expr);
    }

    @Override
    public String getOperatorRepresentation() {
        return "NOT";
    }

    @Override
    public OperatorKind getOperatorKind() {
        return OperatorKind.PREFIX;
    }

}
