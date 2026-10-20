package sqlancer.cloudberry.oracle.tlp;

import java.util.List;

import sqlancer.cloudberry.oracle.CloudberryOracleUtils;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.ast.PostgresExpression;

public class CloudberryTLPGroupByOracle extends CloudberryTLPDeduplicationBase {

    public CloudberryTLPGroupByOracle(PostgresGlobalState state) {
        super(state);
    }

    @Override
    protected void prepareSelect() {
        // Fetch exactly the grouping columns.
        List<PostgresExpression> groupByColumns = CloudberryOracleUtils.randomFetchColumns(targetTables.getColumns());
        select.setFetchColumns(groupByColumns);
        select.setGroupByExpressions(groupByColumns);
    }

}
