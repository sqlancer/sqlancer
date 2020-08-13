package sqlancer.common.ast.newast;

public class NewTernaryNode<T> implements Node<T> {

    protected final Node<T> left;
    protected final Node<T> middle;
    protected final Node<T> right;
    private final String leftStr;
    private final String rightStr;

    public NewTernaryNode(Node<T> left, Node<T> middle, Node<T> right, String leftStr, String rightStr) {
        this.left = left;
        this.middle = middle;
        this.right = right;
        this.leftStr = leftStr;
        this.rightStr = rightStr;
    }

    public Node<T> getLeft() {
        return left;
    }

    public Node<T> getMiddle() {
        return middle;
    }

    public Node<T> getRight() {
        return right;
    }

    public String getLeftStr() {
        return leftStr;
    }

    public String getRightStr() {
        return rightStr;
    }

}
