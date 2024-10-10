package sqlancer.common.oracle;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.SQLGlobalState;
import sqlancer.common.DBMSCommon;
import sqlancer.common.ast.newast.Expression;
import sqlancer.common.ast.newast.Join;
import sqlancer.common.ast.newast.Select;
import sqlancer.common.gen.CERTGenerator;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.common.schema.AbstractSchema;
import sqlancer.common.schema.AbstractTable;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.common.schema.AbstractTables;

public class CERTOracle<Z extends Select<J, E, T, C>, J extends Join<E, T, C>, E extends Expression<C>, S extends AbstractSchema<?, T>, T extends AbstractTable<C, ?, ?>, C extends AbstractTableColumn<?, ?>, G extends SQLGlobalState<?, S>>
        implements TestOracle<G> {

    private final G state;
    private final CheckedFunction<SQLancerResultSet, Optional<Long>> rowCountParser;
    private final CheckedFunction<SQLancerResultSet, Optional<String>> queryPlanParser;

    private CERTGenerator<Z, J, E, T, C> gen;
    private final ExpectedErrors errors;

    public CERTOracle(G state, CERTGenerator<Z, J, E, T, C> gen, ExpectedErrors expectedErrors,
            CheckedFunction<SQLancerResultSet, Optional<Long>> rowCountParser,
            CheckedFunction<SQLancerResultSet, Optional<String>> queryPlanParser) {
        if (state == null || gen == null || expectedErrors == null) {
            throw new IllegalArgumentException("Null variables used to initialize test oracle.");
        }
        this.state = state;
        this.gen = gen;
        this.errors = expectedErrors;
        this.rowCountParser = rowCountParser;
        this.queryPlanParser = queryPlanParser;
    }

    @Override
    public void check() throws SQLException {
        S schema = state.getSchema();
        AbstractTables<T, C> targetTables = TestOracleUtils.getRandomTableNonEmptyTables(schema);
        gen = gen.setTablesAndColumns(targetTables);

        List<E> fetchColumns = gen.generateFetchColumns(false);

        Z select = gen.generateSelect();
        select.setFetchColumns(fetchColumns);
        select.setJoinClauses(gen.getRandomJoinClauses());
        select.setFromList(gen.getTableRefs());

        if (Randomly.getBoolean()) {
            select.setWhereClause(gen.generateBooleanExpression());
        }
        if (Randomly.getBoolean()) {
            select.setGroupByClause(fetchColumns);
            if (Randomly.getBoolean()) {
                select.setHavingClause(gen.generateBooleanExpression());
            }
        }

        List<String> queryPlan1Sequences = new ArrayList<>();
        List<String> queryPlan2Sequences = new ArrayList<>();

        String queryString1 = gen.generateExplainQuery(select);
        long rowCount1 = getRow(state, queryString1, queryPlan1Sequences);

        boolean increase = gen.mutate(select);
        String queryString2 = gen.generateExplainQuery(select);
        long rowCount2 = getRow(state, queryString2, queryPlan2Sequences);

        if (DBMSCommon.editDistance(queryPlan1Sequences, queryPlan2Sequences) > 1) {
            return;
        }

        // Check the results
        if (increase && rowCount1 > rowCount2 || !increase && rowCount1 < rowCount2) {
            throw new AssertionError("Inconsistent result for query: " + queryString1 + "; --" + rowCount1 + "\n"
                    + queryString2 + "; --" + rowCount2);
        }
    }

    private Long getRow(SQLGlobalState<?, ?> globalState, String explainQuery, List<String> queryPlanSequences)
            throws AssertionError, SQLException {
        Optional<Long> row = Optional.empty();

        // Log the query
        if (globalState.getOptions().logEachSelect()) {
            globalState.getLogger().writeCurrent(explainQuery);
            try {
                globalState.getLogger().getCurrentFileWriter().flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        // Get the row count
        SQLQueryAdapter q = new SQLQueryAdapter(explainQuery, errors);
        try (SQLancerResultSet rs = q.executeAndGet(globalState)) {
            if (rs != null) {
                while (rs.next()) {
                    Optional<Long> rowCount = rowCountParser.apply(rs);
                    if (row.isEmpty() && rowCount.isPresent()) {
                        row = rowCount;
                    }

                    Optional<String> queryPlanSequence = queryPlanParser.apply(rs);
                    queryPlanSequence.ifPresent(qps -> queryPlanSequences.add(qps));
                }
            }
        } catch (IgnoreMeException e) {
            throw new IgnoreMeException();
        } catch (Exception e) {
            throw new AssertionError(q.getQueryString(), e);
        }

        return row.orElseThrow(IgnoreMeException::new);
    }

    @FunctionalInterface
    public interface CheckedFunction<T, R> {
        R apply(T t) throws SQLException;
    }
}
