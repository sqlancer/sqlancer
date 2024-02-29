package sqlancer.common.gen;

import java.util.List;
import sqlancer.common.ast.newast.Expression;
import sqlancer.common.ast.newast.Join;
import sqlancer.common.schema.AbstractTable;
import sqlancer.common.schema.AbstractTableColumn;

public interface TLPGenerator<J extends Join<E, T, C>, E extends Expression<C>, T extends AbstractTable<C, ?, ?>, C extends AbstractTableColumn<?, ?>>
        extends SelectGenerator<J, E, T, C>, PredicateGenerator<E, C> {

    TLPGenerator<J, E, T, C> setColumns(List<C> columns);
}
