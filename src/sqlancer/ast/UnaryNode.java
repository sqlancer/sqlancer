package sqlancer.ast;

import sqlancer.visitor.UnaryOperation;

public abstract class UnaryNode<T> implements UnaryOperation<T> {

	protected final T expr;

	public UnaryNode(T expr) {
		this.expr = expr;
	}

	@Override
	public T getExpression() {
		return expr;
	}

}
