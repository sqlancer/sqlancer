package sqlancer.common.ast;

import sqlancer.common.visitor.BinaryOperation;

public abstract class TernaryNode<T> implements BinaryOperation<T> {

    private final T left;
    private final T middle;
    private final T right;

    public TernaryNode(T left, T middle, T right) {
        this.left = left;
        this.middle = middle;
        this.right = right;
    }

    @Override
    public T getLeft() {
        return left;
    }

    public T getMiddle() {
        return middle;
    }

    @Override
    public T getRight() {
        return right;
    }

}
