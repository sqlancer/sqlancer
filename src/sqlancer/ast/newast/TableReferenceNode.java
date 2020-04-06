package sqlancer.ast.newast;

import sqlancer.schema.AbstractTable;

public class TableReferenceNode<E, T extends AbstractTable<?, ?>> implements Node<E> {

	private final T t;

	public TableReferenceNode(T table) {
		this.t = table;
	}
	
	public T getTable() {
		return t;
	}
	
	
}
