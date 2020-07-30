package sqlancer.citus.oracle.tlp;

import java.sql.SQLException;

import sqlancer.citus.CitusGlobalState;
import sqlancer.citus.gen.CitusCommon;
import sqlancer.postgres.oracle.tlp.PostgresTLPHavingOracle;
import sqlancer.postgres.oracle.tlp.PostgresTLPBase;

public class CitusTLPHavingOracle extends PostgresTLPHavingOracle {

    private CitusTLPBase citusTLPBase;

    public CitusTLPHavingOracle(CitusGlobalState state) {
        super(state);
        CitusCommon.addCitusErrors(errors);
        citusTLPBase = (CitusTLPBase)(PostgresTLPBase) this;
    }

    @Override
    public void check() throws SQLException {
        // FIXME: does this affect "this" too?
        citusTLPBase.check();
        havingCheck();
    }
    
}