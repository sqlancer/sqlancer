package sqlancer.citus.oracle.tlp;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import sqlancer.ComparatorHelper;
import sqlancer.Randomly;
import sqlancer.citus.CitusGlobalState;
import sqlancer.citus.gen.CitusCommon;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresVisitor;
import sqlancer.postgres.oracle.tlp.PostgresTLPBase;

public class CitusTLPWhereOracle extends PostgresTLPBase {

    private final CitusTLPBase citusTLPBase;

    public CitusTLPWhereOracle(CitusGlobalState state) {
        super(state);
        CitusCommon.addCitusErrors(errors);
        citusTLPBase = new CitusTLPBase(state);
    }

    @Override
    public void check() throws SQLException {
        state.setAllowedFunctionTypes(Arrays.asList(PostgresGlobalState.IMMUTABLE));
        citusTLPBase.check();
        s = citusTLPBase.getSchema();
        targetTables = citusTLPBase.getTargetTables();
        gen = citusTLPBase.getGenerator();
        select = citusTLPBase.getSelect();
        predicate = citusTLPBase.getPredicate();
        negatedPredicate = citusTLPBase.getNegatedPredicate();
        isNullPredicate = citusTLPBase.getIsNullPredicate();
        whereCheck();
        state.setDefaultAllowedFunctionTypes();
    }

    void whereCheck() throws SQLException {
        if (Randomly.getBooleanWithRatherLowProbability()) {
            select.setOrderByClauses(gen.generateOrderBys());
        }
        String originalQueryString = PostgresVisitor.asString(select);
        List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);

        select.setOrderByClauses(Collections.emptyList());
        select.setWhereClause(predicate);
        String firstQueryString = PostgresVisitor.asString(select);
        select.setWhereClause(negatedPredicate);
        String secondQueryString = PostgresVisitor.asString(select);
        select.setWhereClause(isNullPredicate);
        String thirdQueryString = PostgresVisitor.asString(select);
        List<String> combinedString = new ArrayList<>();
        List<String> secondResultSet = ComparatorHelper.getCombinedResultSet(firstQueryString, secondQueryString,
                thirdQueryString, combinedString, Randomly.getBoolean(), state, errors);
        ComparatorHelper.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQueryString, combinedString,
                state);
    }
}
