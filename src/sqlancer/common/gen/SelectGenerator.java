package sqlancer.common.gen;

import java.util.List;

import sqlancer.common.ast.newast.Expression;
import sqlancer.common.ast.newast.Join;
import sqlancer.common.ast.newast.Select;
import sqlancer.common.schema.AbstractTable;
import sqlancer.common.schema.AbstractTableColumn;

public interface SelectGenerator<J extends Join<E, T, C>, E extends Expression<C>, T extends AbstractTable<C, ?, ?>, C extends AbstractTableColumn<?, ?>> {

    Select<J, E, T, C> generateSelect();

    List<E> generateFetchColumns();

    List<E> generateOrderBys();

    List<J> getRandomJoinClauses();

    List<E> getTableRefs();
}
