package sqlancer.presto.ast;

import sqlancer.common.ast.newast.Node;

public class PrestoQuantifiedComparison implements Node<PrestoExpression> {

    private final Node<PrestoExpression> left;
    private final PrestoSelect right;
    private final PrestoMultiValuedComparisonType type;
    private final PrestoMultiValuedComparisonOperator op;

    public PrestoQuantifiedComparison(Node<PrestoExpression> left, PrestoSelect right,
            PrestoMultiValuedComparisonType type, PrestoMultiValuedComparisonOperator op) {
        this.left = left;
        this.right = right;
        this.type = type;
        this.op = op;
    }

    public Node<PrestoExpression> getLeft() {
        return left;
    }

    public PrestoMultiValuedComparisonOperator getOp() {
        return op;
    }

    public Node<PrestoExpression> getRight() {
        return right;
    }

    public PrestoMultiValuedComparisonType getType() {
        return type;
    }

}
