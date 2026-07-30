package sqlancer.common.gen;

import java.util.List;

import sqlancer.common.ast.newast.Expression;
import sqlancer.common.ast.newast.Join;
import sqlancer.common.ast.newast.Select;
import sqlancer.common.oracle.EETTransformer;
import sqlancer.common.schema.AbstractTable;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.common.schema.AbstractTables;

/**
 * Generator interface used by {@link sqlancer.common.oracle.EETOracle}. In addition to generating a random query (like
 * the other oracle generators), an EET generator creates a DBMS-specific {@link EETTransformer} that the oracle uses to
 * rewrite expressions into semantically equivalent ones.
 *
 * @param <S>
 *            the DBMS-specific SELECT statement class
 * @param <J>
 *            the DBMS-specific JOIN clause class
 * @param <E>
 *            the DBMS-specific expression class
 * @param <T>
 *            the DBMS-specific table class
 * @param <C>
 *            the DBMS-specific column class
 */
public interface EETGenerator<S extends Select<J, E, T, C>, J extends Join<E, T, C>, E extends Expression<C>, T extends AbstractTable<C, ?, ?>, C extends AbstractTableColumn<?, ?>> {

    EETGenerator<S, J, E, T, C> setTablesAndColumns(AbstractTables<T, C> tables);

    S generateSelect();

    List<J> getRandomJoinClauses();

    List<E> getTableRefs();

    List<E> generateFetchColumns(boolean shouldCreateDummy);

    E generateBooleanExpression();

    /**
     * Creates a DBMS-specific {@link EETTransformer} backed by this generator. Called once by
     * {@link sqlancer.common.oracle.EETOracle} during construction; the oracle owns the returned transformer for the
     * lifetime of the test run.
     *
     * @return a DBMS-specific {@link EETTransformer} backed by this generator
     */
    EETTransformer<E, ?> createTransformer();
}
