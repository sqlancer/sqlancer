package sqlancer.citus.oracle.tlp;

import java.sql.SQLException;

import sqlancer.citus.CitusGlobalState;
import sqlancer.citus.gen.CitusCommon;
import sqlancer.postgres.oracle.tlp.PostgresTLPAggregateOracle;
import sqlancer.postgres.oracle.tlp.PostgresTLPBase;

public class CitusTLPAggregateOracle extends PostgresTLPAggregateOracle {

    private CitusTLPBase citusTLPBase;

    public CitusTLPAggregateOracle(CitusGlobalState state) {
        super(state);
        CitusCommon.addCitusErrors(errors);
        citusTLPBase = (CitusTLPBase)(PostgresTLPBase) this;
    }

    @Override
    public void check() throws SQLException {
        // FIXME: does this affect "this" too?
        citusTLPBase.check();
        aggregateCheck();
    }

}