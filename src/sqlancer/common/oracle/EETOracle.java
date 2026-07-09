package sqlancer.common.oracle;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.ComparatorHelper;
import sqlancer.Reproducer;
import sqlancer.SQLGlobalState;
import sqlancer.common.ast.newast.Expression;
import sqlancer.common.ast.newast.Join;
import sqlancer.common.ast.newast.Select;
import sqlancer.common.gen.EETGenerator;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.schema.AbstractSchema;
import sqlancer.common.schema.AbstractTable;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.common.schema.AbstractTables;

/**
 * EET (Equivalent Expression Transformation) oracle, based on "Detecting Logic Bugs in Database Engines via Equivalent
 * Expression Transformation" (Jiang &amp; Su, OSDI'24).
 *
 * <p>
 * The oracle generates a random query and then transforms its expressions (the WHERE predicate and the fetch columns)
 * into semantically equivalent ones using {@link EETGenerator#transformExpression}. Because the transformation preserves
 * semantics, the original and the transformed query must return the same result set; any discrepancy indicates a logic
 * bug in the DBMS.
 */
public class EETOracle<Z extends Select<J, E, T, C>, J extends Join<E, T, C>, E extends Expression<C>, S extends AbstractSchema<?, T>, T extends AbstractTable<C, ?, ?>, C extends AbstractTableColumn<?, ?>, G extends SQLGlobalState<?, S>>
        implements TestOracle<G> {

    private final G state;
    private EETGenerator<Z, J, E, T, C> gen;
    private final ExpectedErrors errors;

    private Reproducer<G> reproducer;
    private String generatedQueryString;

    private final class EETReproducer implements Reproducer<G> {
        private final String originalQueryString;
        private final String transformedQueryString;
        private final List<String> resultSet;

        EETReproducer(String originalQueryString, String transformedQueryString, List<String> resultSet) {
            this.originalQueryString = originalQueryString;
            this.transformedQueryString = transformedQueryString;
            this.resultSet = resultSet;
        }

        @Override
        public boolean bugStillTriggers(G globalState) {
            try {
                List<String> transformedResultSet = ComparatorHelper
                        .getResultSetFirstColumnAsString(transformedQueryString, errors, globalState);
                ComparatorHelper.assumeResultSetsAreEqual(resultSet, transformedResultSet, originalQueryString,
                        List.of(transformedQueryString), globalState);
            } catch (AssertionError triggeredError) {
                return true;
            } catch (SQLException ignored) {
            }
            return false;
        }
    }

    public EETOracle(G state, EETGenerator<Z, J, E, T, C> gen, ExpectedErrors expectedErrors) {
        if (state == null || gen == null || expectedErrors == null) {
            throw new IllegalArgumentException("Null variables used to initialize test oracle.");
        }
        this.state = state;
        this.gen = gen;
        this.errors = expectedErrors;
    }

    @Override
    public void check() throws SQLException {
        reproducer = null;
        S schema = state.getSchema();
        AbstractTables<T, C> targetTables = TestOracleUtils.getRandomTableNonEmptyTables(schema);
        gen = gen.setTablesAndColumns(targetTables);

        Z select = gen.generateSelect();
        select.setJoinClauses(gen.getRandomJoinClauses());
        select.setFromList(gen.getTableRefs());
        List<E> fetchColumns = gen.generateFetchColumns(true);
        select.setFetchColumns(fetchColumns);
        E whereClause = gen.generateBooleanExpression();
        select.setWhereClause(whereClause);

        String originalQueryString = select.asString();
        generatedQueryString = originalQueryString;
        List<String> originalResultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors,
                state);

        // Transform the query's expressions into semantically equivalent ones. Fetch columns are scalar expressions,
        // while the WHERE clause is evaluated in a boolean context.
        List<E> transformedFetchColumns = fetchColumns.stream().map(c -> gen.transformExpression(c, false))
                .collect(Collectors.toList());
        select.setFetchColumns(transformedFetchColumns);
        select.setWhereClause(gen.transformExpression(whereClause, true));

        String transformedQueryString = select.asString();
        List<String> transformedResultSet = ComparatorHelper.getResultSetFirstColumnAsString(transformedQueryString,
                errors, state);

        ComparatorHelper.assumeResultSetsAreEqual(originalResultSet, transformedResultSet, originalQueryString,
                List.of(transformedQueryString), state);

        reproducer = new EETReproducer(originalQueryString, transformedQueryString, originalResultSet);
    }

    @Override
    public Reproducer<G> getLastReproducer() {
        return reproducer;
    }

    @Override
    public String getLastQueryString() {
        return generatedQueryString;
    }
}
