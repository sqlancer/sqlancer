package sqlancer.cockroachdb.ast;

import sqlancer.common.ast.UnaryNode;

public class CockroachDBCollate extends UnaryNode<CockroachDBExpression> implements CockroachDBExpression {

    private final String collate;

    public CockroachDBCollate(CockroachDBExpression expr, String collate) {
        super(expr);
        this.collate = collate;
    }

    public String getCollate() {
        return collate;
    }

    @Override
    public String getOperatorRepresentation() {
        return "COLLATE " + collate;
    }

    @Override
    public OperatorKind getOperatorKind() {
        return OperatorKind.POSTFIX;
    }

}
