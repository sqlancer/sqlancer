package sqlancer.common.gen;

import java.util.List;

import sqlancer.common.ast.newast.Aggregate;
import sqlancer.common.ast.newast.Expression;
import sqlancer.common.schema.AbstractTable;
import sqlancer.common.schema.AbstractTableColumn;

public interface AggregateGenerator<A extends Aggregate<E, C>, E extends Expression<C>, T extends AbstractTable<C, ?, ?>, C extends AbstractTableColumn<?, ?>> {

    A generateAggregate();

    List<E> aliasAggregates(List<A> aggregates);
}
