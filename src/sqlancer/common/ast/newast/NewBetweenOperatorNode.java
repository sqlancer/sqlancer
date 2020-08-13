package sqlancer.common.ast.newast;

public class NewBetweenOperatorNode<T> implements Node<T> {

    protected Node<T> left;
    protected Node<T> middle;
    protected Node<T> right;
    protected boolean isTrue;

    public NewBetweenOperatorNode(Node<T> left, Node<T> middle, Node<T> right, boolean isTrue) {
        this.left = left;
        this.middle = middle;
        this.right = right;
        this.isTrue = isTrue;
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

    public boolean isTrue() {
        return isTrue;
    }

}
