package sqlancer.common.oracle;

import java.sql.SQLException;
import java.util.Arrays;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.SQLGlobalState;
import sqlancer.common.ast.newast.Expression;
import sqlancer.common.ast.newast.Join;
import sqlancer.common.ast.newast.Select;
import sqlancer.common.gen.NoRECGenerator;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.common.schema.AbstractSchema;
import sqlancer.common.schema.AbstractTable;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.common.schema.AbstractTables;

public class NoRECOracle<J extends Join<E, T, C>, E extends Expression<C>, S extends AbstractSchema<?, T>, T extends AbstractTable<C, ?, ?>, C extends AbstractTableColumn<?, ?>, G extends SQLGlobalState<?, S>>
        implements TestOracle<G> {

    private final G state;

    private NoRECGenerator<J, E, T, C> gen;
    private final ExpectedErrors errors;

    private String lastQueryString;

    public NoRECOracle(G state, NoRECGenerator<J, E, T, C> gen, ExpectedErrors expectedErrors) {
        this.state = state;
        this.gen = gen;
        this.errors = expectedErrors;
    }

    @Override
    public void check() throws SQLException {
        S schema = state.getSchema();
        AbstractTables<T, C> targetTables = TestOracleUtils.getRandomTableNonEmptyTables(schema);
        gen = gen.setTablesAndColumns(targetTables);

        Select<J, E, T, C> select = gen.generateSelect();
        select.setJoinClauses(gen.getRandomJoinClauses());
        select.setFromList(gen.getTableRefs());

        E randomWhereCondition = gen.generateBooleanExpression();

        boolean shouldUseAggregate = Randomly.getBoolean();
        select.setFetchColumns(Arrays.asList(gen.generateOptimizedFetchColumn(shouldUseAggregate)));
        select.setWhereClause(randomWhereCondition);
        String optimizedQueryString = select.asString();
        lastQueryString = optimizedQueryString;

        select.setFetchColumns(Arrays.asList(gen.generateUnoptimizedFetchColumn(randomWhereCondition)));
        select.setWhereClause(null);
        String unoptimizedQueryString = select.toSum().toString();

        int optimizedCount = shouldUseAggregate ? extractCounts(optimizedQueryString, errors, state)
                : countRows(optimizedQueryString, errors, state);
        int unoptimizedCount = extractCounts(optimizedQueryString, errors, state);

        if (optimizedCount == -1 || unoptimizedCount == -1) {
            throw new IgnoreMeException();
        }

        if (unoptimizedCount != optimizedCount) {
            String queryFormatString = "-- %s;\n-- count: %d";
            String firstQueryStringWithCount = String.format(queryFormatString, optimizedQueryString, optimizedCount);
            String secondQueryStringWithCount = String.format(queryFormatString, unoptimizedQueryString,
                    unoptimizedCount);
            state.getState().getLocalState()
                    .log(String.format("%s\n%s", firstQueryStringWithCount, secondQueryStringWithCount));
            String assertionMessage = String.format("the counts mismatch (%d and %d)!\n%s\n%s", optimizedCount,
                    unoptimizedCount, firstQueryStringWithCount, secondQueryStringWithCount);
            throw new AssertionError(assertionMessage);
        }
    }

    @Override
    public String getLastQueryString() {
        return lastQueryString;
    }

    private int countRows(String queryString, ExpectedErrors errors, SQLGlobalState<?, ?> state) {
        SQLQueryAdapter q = new SQLQueryAdapter(queryString, errors);

        if (state.getOptions().logEachSelect()) {
            state.getLogger().writeCurrent(queryString);
        }

        int count = 0;
        try (SQLancerResultSet rs = q.executeAndGet(state)) {
            if (rs == null) {
                return -1;
            } else {
                try {
                    while (rs.next()) {
                        count++;
                    }
                } catch (SQLException e) {
                    count = -1;
                }
            }
        } catch (Exception e) {
            if (e instanceof IgnoreMeException) {
                throw (IgnoreMeException) e;
            }
            throw new AssertionError(q.getQueryString(), e);
        }
        return count;
    }

    private int extractCounts(String queryString, ExpectedErrors errors, SQLGlobalState<?, ?> state) {
        SQLQueryAdapter q = new SQLQueryAdapter(queryString, errors);
        if (state.getOptions().logEachSelect()) {
            state.getLogger().writeCurrent(queryString);
        }

        int count = 0;
        try (SQLancerResultSet rs = q.executeAndGet(state)) {
            if (rs == null) {
                return -1;
            } else {
                try {
                    while (rs.next()) {
                        count += rs.getInt(1);
                    }
                } catch (SQLException e) {
                    count = -1;
                }
            }
        } catch (Exception e) {
            if (e instanceof IgnoreMeException) {
                throw (IgnoreMeException) e;
            }
            throw new AssertionError(q.getQueryString(), e);
        }
        return count;
    }

}
