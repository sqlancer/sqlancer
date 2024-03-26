package sqlancer.common.gen;

import java.util.List;

import sqlancer.common.ast.newast.Expression;
import sqlancer.common.ast.newast.Join;
import sqlancer.common.ast.newast.Select;
import sqlancer.common.schema.AbstractTable;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.common.schema.AbstractTables;

public interface NoRECGenerator<J extends Join<E, T, C>, E extends Expression<C>, T extends AbstractTable<C, ?, ?>, C extends AbstractTableColumn<?, ?>> {

    NoRECGenerator<J, E, T, C> setTablesAndColumns(AbstractTables<T, C> tables);

    E generateBooleanExpression();

    Select<J, E, T, C> generateSelect();

    List<J> getRandomJoinClauses();

    List<E> getTableRefs();

    E generateOptimizedFetchColumn(boolean shouldUseAggregate);

    E generateUnoptimizedFetchColumn(E whereCondition);
}
