package sqlancer.presto.ast;

public class PrestoQuantifiedComparison implements PrestoExpression {

    private final PrestoExpression left;
    private final PrestoSelect right;
    private final PrestoMultiValuedComparisonType type;
    private final PrestoMultiValuedComparisonOperator op;

    public PrestoQuantifiedComparison(PrestoExpression left, PrestoSelect right, PrestoMultiValuedComparisonType type,
            PrestoMultiValuedComparisonOperator op) {
        this.left = left;
        this.right = right;
        this.type = type;
        this.op = op;
    }

    public PrestoExpression getLeft() {
        return left;
    }

    public PrestoMultiValuedComparisonOperator getOp() {
        return op;
    }

    public PrestoExpression getRight() {
        return right;
    }

    public PrestoMultiValuedComparisonType getType() {
        return type;
    }

}
