package sqlancer.common.ast.newast;

public class NewTernaryNode<T> {

    protected final T left;
    protected final T middle;
    protected final T right;
    private final String leftStr;
    private final String rightStr;

    public NewTernaryNode(T left, T middle, T right, String leftStr, String rightStr) {
        this.left = left;
        this.middle = middle;
        this.right = right;
        this.leftStr = leftStr;
        this.rightStr = rightStr;
    }

    public T getLeft() {
        return left;
    }

    public T getMiddle() {
        return middle;
    }

    public T getRight() {
        return right;
    }

    public String getLeftStr() {
        return leftStr;
    }

    public String getRightStr() {
        return rightStr;
    }

}
