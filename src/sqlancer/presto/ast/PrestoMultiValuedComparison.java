package sqlancer.presto.ast;

import java.util.ArrayList;
import java.util.List;

import sqlancer.common.ast.newast.Node;

public class PrestoMultiValuedComparison implements Node<PrestoExpression> {

    private final Node<PrestoExpression> left;
    private final List<Node<PrestoExpression>> right;
    private final PrestoMultiValuedComparisonType type;
    private final PrestoMultiValuedComparisonOperator op;

    public PrestoMultiValuedComparison(Node<PrestoExpression> left, List<Node<PrestoExpression>> right,
            PrestoMultiValuedComparisonType type, PrestoMultiValuedComparisonOperator op) {
        this.left = left;
        this.right = new ArrayList<>(right);
        this.type = type;
        this.op = op;
    }

    public Node<PrestoExpression> getLeft() {
        return left;
    }

    public PrestoMultiValuedComparisonOperator getOp() {
        return op;
    }

    public List<Node<PrestoExpression>> getRight() {
        return new ArrayList<>(right);
    }

    public PrestoMultiValuedComparisonType getType() {
        return type;
    }

}
