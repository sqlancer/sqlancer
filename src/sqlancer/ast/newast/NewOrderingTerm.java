package sqlancer.ast.newast;

import sqlancer.Randomly;

public class NewOrderingTerm<T> implements Node<T> {

	private Node<T> expr;
	private Ordering ordering;

	public enum Ordering {
		ASC, DESC;

		public static Ordering getRandom() {
			return Randomly.fromOptions(values());
		}
	}
	
	public NewOrderingTerm(Node<T> expr, Ordering ordering) {
		this.expr = expr;
		this.ordering = ordering;
	}
	
	public Node<T> getExpr() {
		return expr;
	}
	
	public Ordering getOrdering() {
		return ordering;
	}
	
}
