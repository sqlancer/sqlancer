package sqlancer.ast.newast;

import java.util.List;

public class NewCaseOperatorNode<T> implements Node<T> {

	private List<Node<T>> conditions;
	private List<Node<T>> expressions;
	private Node<T> elseExpr;
	private Node<T> switchCondition;

	public NewCaseOperatorNode(Node<T> switchCondition, List<Node<T>> conditions, List<Node<T>> expressions, Node<T> elseExpr) {
		this.switchCondition = switchCondition;
		this.conditions = conditions;
		this.expressions = expressions;
		this.elseExpr = elseExpr;
		if (conditions.size() != expressions.size()) {
			throw new IllegalArgumentException();
		}
	}
	
	public Node<T> getSwitchCondition() {
		return switchCondition;
	}
	
	public List<Node<T>> getConditions() {
		return conditions;
	}
	public List<Node<T>> getExpressions() {
		return expressions;
	}
	
	public Node<T> getElseExpr() {
		return elseExpr;
	}
	
}
