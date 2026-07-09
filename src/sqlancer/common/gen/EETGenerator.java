package sqlancer.common.gen;

import java.util.List;

import sqlancer.common.ast.newast.Expression;
import sqlancer.common.ast.newast.Join;
import sqlancer.common.ast.newast.Select;
import sqlancer.common.schema.AbstractTable;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.common.schema.AbstractTables;

/**
 * Generator interface used by {@link sqlancer.common.oracle.EETOracle}. In addition to generating a random query (like
 * the other oracle generators), an EET generator can transform an expression into a semantically equivalent one
 * according to the EET transformation rules.
 */
public interface EETGenerator<S extends Select<J, E, T, C>, J extends Join<E, T, C>, E extends Expression<C>, T extends AbstractTable<C, ?, ?>, C extends AbstractTableColumn<?, ?>> {

    EETGenerator<S, J, E, T, C> setTablesAndColumns(AbstractTables<T, C> tables);

    S generateSelect();

    List<J> getRandomJoinClauses();

    List<E> getTableRefs();

    List<E> generateFetchColumns(boolean shouldCreateDummy);

    E generateBooleanExpression();

    /**
     * Transforms an expression into a semantically equivalent one (the core of EET). Typically this recursively
     * traverses the expression's AST and replaces sub-expressions with equivalent ones.
     *
     * @param expr
     *            the expression to transform
     * @param booleanContext
     *            whether {@code expr} is evaluated purely for its truth value (e.g. a WHERE predicate); this controls
     *            which transformation rules are applicable
     *
     * @return a semantically equivalent expression
     */
    E transformExpression(E expr, boolean booleanContext);
}
