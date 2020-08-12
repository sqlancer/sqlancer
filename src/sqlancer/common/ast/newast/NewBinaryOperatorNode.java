package sqlancer.common.ast.newast;

import sqlancer.common.ast.BinaryOperatorNode.Operator;

public class NewBinaryOperatorNode<T> implements Node<T> {

    protected final Operator op;
    protected final Node<T> left;
    protected final Node<T> right;

    public NewBinaryOperatorNode(Node<T> left, Node<T> right, Operator op) {
        this.left = left;
        this.right = right;
        this.op = op;
    }

    public String getOperatorRepresentation() {
        return op.getTextRepresentation();
    }

    public Node<T> getLeft() {
        return left;
    }

    public Node<T> getRight() {
        return right;
    }

}
