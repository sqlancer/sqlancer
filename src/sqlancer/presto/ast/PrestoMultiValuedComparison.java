package sqlancer.presto.ast;

import java.util.ArrayList;
import java.util.List;

public class PrestoMultiValuedComparison implements PrestoExpression {

    private final PrestoExpression left;
    private final List<PrestoExpression> right;
    private final PrestoMultiValuedComparisonType type;
    private final PrestoMultiValuedComparisonOperator op;

    public PrestoMultiValuedComparison(PrestoExpression left, List<PrestoExpression> right,
            PrestoMultiValuedComparisonType type, PrestoMultiValuedComparisonOperator op) {
        this.left = left;
        this.right = new ArrayList<>(right);
        this.type = type;
        this.op = op;
    }

    public PrestoExpression getLeft() {
        return left;
    }

    public PrestoMultiValuedComparisonOperator getOp() {
        return op;
    }

    public List<PrestoExpression> getRight() {
        return new ArrayList<>(right);
    }

    public PrestoMultiValuedComparisonType getType() {
        return type;
    }

}
