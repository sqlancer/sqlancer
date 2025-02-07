package sqlancer.citus.oracle.tlp;

import java.sql.SQLException;

import sqlancer.citus.CitusGlobalState;
import sqlancer.postgres.oracle.tlp.PostgresTLPHavingOracle;

public class CitusTLPHavingOracle extends PostgresTLPHavingOracle {

    private final CitusTLPBase citusTLPBase;

    public CitusTLPHavingOracle(CitusGlobalState state) {
        super(state);
        citusTLPBase = CitusTLPBase.createWithState(state, errors);
    }

    @Override
    public void check() throws SQLException {
        citusTLPBase.initializeState(state);
        havingCheck();
        state.setDefaultAllowedFunctionTypes();
    }

}
