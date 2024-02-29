package sqlancer.common.ast.newast;

import sqlancer.common.schema.AbstractTableColumn;

public interface Constant<C extends AbstractTableColumn<?, ?>> extends Expression<C> {
}
