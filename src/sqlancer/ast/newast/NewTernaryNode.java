package sqlancer.ast.newast;

public class NewTernaryNode<T> implements Node<T> {

    protected Node<T> left;
    protected Node<T> middle;
    protected Node<T> right;
    private String leftStr;
    private String rightStr;

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
