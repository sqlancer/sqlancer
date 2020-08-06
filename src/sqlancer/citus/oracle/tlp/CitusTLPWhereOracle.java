package sqlancer.citus.oracle.tlp;

import java.sql.SQLException;

import sqlancer.citus.CitusGlobalState;
import sqlancer.citus.gen.CitusCommon;
import sqlancer.postgres.oracle.tlp.PostgresTLPWhereOracle;
import sqlancer.postgres.oracle.tlp.PostgresTLPBase;

public class CitusTLPWhereOracle extends PostgresTLPWhereOracle {

    private CitusTLPBase citusTLPBase;

    public CitusTLPWhereOracle(CitusGlobalState state) {
        super(state);
        CitusCommon.addCitusErrors(errors);
        citusTLPBase = new CitusTLPBase(state);
    }

    @Override
    public void check() throws SQLException {
        citusTLPBase.check();
        s = citusTLPBase.getSchema();
        targetTables = citusTLPBase.getTargetTables();
        gen = citusTLPBase.getGenerator();
        select = citusTLPBase.getSelect();
        whereCheck();
    }
}