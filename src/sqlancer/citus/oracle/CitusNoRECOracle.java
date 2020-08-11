package sqlancer.citus.oracle;

import sqlancer.citus.gen.CitusCommon;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.oracle.PostgresNoRECOracle;

public class CitusNoRECOracle extends PostgresNoRECOracle {

    public CitusNoRECOracle(PostgresGlobalState globalState) {
        super(globalState);
        CitusCommon.addCitusErrors(errors);
    }

}
