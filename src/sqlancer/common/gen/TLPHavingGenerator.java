package sqlancer.common.gen;

import java.util.List;

import sqlancer.common.ast.newast.Expression;
import sqlancer.common.ast.newast.Join;
import sqlancer.common.schema.AbstractTable;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.common.schema.AbstractTables;

public interface TLPHavingGenerator<J extends Join<E, T, C>, E extends Expression<C>, T extends AbstractTable<C, ?, ?>, C extends AbstractTableColumn<?, ?>>
        extends SelectGenerator<J, E, T, C>, HavingClauseGenerator<E, C>, TypeExpressionGenerator<E, T, C> {

    TLPHavingGenerator<J, E, T, C> setColumns(List<C> columns);

    TLPHavingGenerator<J, E, T, C> setTablesAndColumns(AbstractTables<T, C> tables);

    String combineQueryStrings(String... queryStrings);
}
