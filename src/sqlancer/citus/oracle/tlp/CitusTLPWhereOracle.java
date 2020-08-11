package sqlancer.citus.oracle.tlp;

import java.sql.SQLException;
import java.util.Arrays;

import sqlancer.citus.CitusGlobalState;
import sqlancer.citus.gen.CitusCommon;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.oracle.tlp.PostgresTLPWhereOracle;

public class CitusTLPWhereOracle extends PostgresTLPWhereOracle {

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
}
