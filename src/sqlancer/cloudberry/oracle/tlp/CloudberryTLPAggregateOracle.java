package sqlancer.cloudberry.oracle.tlp;

import java.util.List;

import sqlancer.cloudberry.gen.CloudberryCommon;
import sqlancer.cloudberry.oracle.CloudberryOracleUtils;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresSchema.PostgresTable;
import sqlancer.postgres.ast.PostgresJoin;
import sqlancer.postgres.oracle.tlp.PostgresTLPAggregateOracle;

public class CloudberryTLPAggregateOracle extends PostgresTLPAggregateOracle {

    public CloudberryTLPAggregateOracle(PostgresGlobalState state) {
        super(state);
        CloudberryCommon.addCloudberryErrors(errors);
    }

    @Override
    protected void generateSelectBase(List<PostgresTable> tables, List<PostgresJoin> joins) {
        super.generateSelectBase(tables, joins);
        CloudberryOracleUtils.removeOnly(select);
    }

}
