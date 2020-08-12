package sqlancer.cockroachdb.ast;

import sqlancer.common.ast.BinaryNode;

public class CockroachDBConcatOperation extends BinaryNode<CockroachDBExpression> implements CockroachDBExpression {

    public CockroachDBConcatOperation(CockroachDBExpression left, CockroachDBExpression right) {
        super(left, right);
    }

    @Override
    public String getOperatorRepresentation() {
        return "||";
    }

}
