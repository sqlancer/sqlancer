package sqlancer.common.ast.newast;

import sqlancer.common.schema.AbstractTableColumn;

public class ColumnReferenceNode<E, C extends AbstractTableColumn<?, ?>> implements Node<E> {

    private final C c;

    public ColumnReferenceNode(C c) {
        this.c = c;
    }

    public C getColumn() {
        return c;
    }

}
