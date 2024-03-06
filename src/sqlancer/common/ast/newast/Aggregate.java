package sqlancer.common.ast.newast;

import sqlancer.common.schema.AbstractTableColumn;

public interface Aggregate<E extends Expression<C>, C extends AbstractTableColumn<?, ?>> extends Expression<C> {

    E asExpression();

    String asString();

    String asAggregatedString(String... from);
}
