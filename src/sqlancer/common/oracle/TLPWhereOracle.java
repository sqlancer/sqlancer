package sqlancer.common.oracle;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import sqlancer.ComparatorHelper;
import sqlancer.Randomly;
import sqlancer.SQLGlobalState;
import sqlancer.common.ast.newast.Expression;
import sqlancer.common.ast.newast.Join;
import sqlancer.common.ast.newast.Select;
import sqlancer.common.gen.TLPGenerator;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.schema.AbstractSchema;
import sqlancer.common.schema.AbstractTable;
import sqlancer.common.schema.AbstractTableColumn;

public class TLPWhereOracle<J extends Join<E, T, C>, E extends Expression<C>, S extends AbstractSchema<?, T>, T extends AbstractTable<C, ?, ?>, C extends AbstractTableColumn<?, ?>, G extends SQLGlobalState<?, S>>
        implements TestOracle<G> {

    private final G state;

    private TLPGenerator<J, E, T, C> gen;
    private final ExpectedErrors errors;

    private String generatedQueryString;

    public TLPWhereOracle(G state, TLPGenerator<J, E, T, C> gen) {
        this.state = state;
        this.gen = gen;
        this.errors = new ExpectedErrors();
    }

    @Override
    public void check() throws SQLException {
        gen = TestOracleUtils.initializeGenerator(state, gen);

        Select<J, E, T, C> select = gen.generateSelect();

        select.setFetchColumns(gen.generateFetchColumns());
        select.setJoinClauses(gen.getRandomJoinClauses());
        select.setFromTables(gen.getTableRefs());
        select.setWhereClause(null);

        String originalQueryString = select.asString();
        generatedQueryString = originalQueryString;

        boolean orderBy = Randomly.getBooleanWithSmallProbability();
        if (orderBy) {
            select.setOrderByExpressions(gen.generateOrderBys());
        }

        TestOracleUtils.PredicateVariants<E, C> predicates = TestOracleUtils.initializeTernaryPredicateVariants(gen);
        select.setWhereClause(predicates.predicate);
        String firstQueryString = select.asString();
        select.setWhereClause(predicates.negatedPredicate);
        String secondQueryString = select.asString();
        select.setWhereClause(predicates.isNullPredicate);
        String thirdQueryString = select.asString();

        List<String> firstResultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors,
                state);
        List<String> combinedString = new ArrayList<>();
        List<String> secondResultSet = ComparatorHelper.getCombinedResultSet(firstQueryString, secondQueryString,
                thirdQueryString, combinedString, !orderBy, state, errors);

        ComparatorHelper.assumeResultSetsAreEqual(firstResultSet, secondResultSet, originalQueryString, combinedString,
                state);
    }

    @Override
    public String getLastQueryString() {
        return generatedQueryString;
    }
}
