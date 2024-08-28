package sqlancer.common.ast.newast;

import java.util.List;

public class NewInOperatorNode<T> {

    private final T left;
    private final List<T> right;
    private final boolean isNegated;

    public NewInOperatorNode(T left, List<T> right, boolean isNegated) {
        this.left = left;
        this.right = right;
        this.isNegated = isNegated;
    }

    public T getLeft() {
        return left;
    }

    public List<T> getRight() {
        return right;
    }

    public boolean isNegated() {
        return isNegated;
    }

}
