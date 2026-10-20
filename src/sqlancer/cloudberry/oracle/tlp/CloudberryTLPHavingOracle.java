package sqlancer.cloudberry.oracle.tlp;

import java.util.List;

import sqlancer.cloudberry.gen.CloudberryCommon;
import sqlancer.cloudberry.oracle.CloudberryOracleUtils;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresSchema.PostgresTable;
import sqlancer.postgres.ast.PostgresJoin;
import sqlancer.postgres.oracle.tlp.PostgresTLPHavingOracle;

public class CloudberryTLPHavingOracle extends PostgresTLPHavingOracle {

    public CloudberryTLPHavingOracle(PostgresGlobalState state) {
        super(state);
        CloudberryCommon.addCloudberryErrors(errors);
    }

    @Override
    protected void generateSelectBase(List<PostgresTable> tables, List<PostgresJoin> joins) {
        super.generateSelectBase(tables, joins);
        CloudberryOracleUtils.removeOnly(select);
    }

}
