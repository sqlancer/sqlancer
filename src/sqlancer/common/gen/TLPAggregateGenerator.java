package sqlancer.common.gen;

import java.util.List;

import sqlancer.common.ast.newast.Aggregate;
import sqlancer.common.ast.newast.Expression;
import sqlancer.common.ast.newast.Join;
import sqlancer.common.schema.AbstractTable;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.common.schema.AbstractTables;

public interface TLPAggregateGenerator<A extends Aggregate<E, C>, J extends Join<E, T, C>, E extends Expression<C>, T extends AbstractTable<C, ?, ?>, C extends AbstractTableColumn<?, ?>>
        extends SelectGenerator<J, E, T, C>, PredicateGenerator<E, C>, AggregateGenerator<A, E, T, C> {

    TLPAggregateGenerator<A, J, E, T, C> setColumns(List<C> columns);

    TLPAggregateGenerator<A, J, E, T, C> setTablesAndColumns(AbstractTables<T, C> tables);

    E generateExpression();

    List<E> getRandomExpressions(int size);
}
