package sqlancer.citus.oracle.tlp;

import java.sql.SQLException;
import java.util.Arrays;

import sqlancer.citus.CitusGlobalState;
import sqlancer.citus.gen.CitusCommon;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.oracle.tlp.PostgresTLPAggregateOracle;

public class CitusTLPAggregateOracle extends PostgresTLPAggregateOracle {

    private final CitusTLPBase citusTLPBase;

    public CitusTLPAggregateOracle(CitusGlobalState state) {
        super(state);
        citusTLPBase = CitusTLPBase.createWithState(state, errors);
    }

    @Override
    public void check() throws SQLException {
        citusTLPBase.initializeState(state);
        aggregateCheck();
        state.setDefaultAllowedFunctionTypes();
    }

}
