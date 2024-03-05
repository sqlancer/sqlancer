package sqlancer.common.gen;

import sqlancer.common.ast.newast.Expression;
import sqlancer.common.schema.AbstractTableColumn;

public interface HavingClauseGenerator<E extends Expression<C>, C extends AbstractTableColumn<?, ?>> {

    E getHavingClause();

    E negatePredicate(E predicate);

    E isNull(E expr);
}
