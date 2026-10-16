package sqlancer.cloudberry.oracle.tlp;

import sqlancer.cloudberry.oracle.CloudberryOracleUtils;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.ast.PostgresSelect.SelectType;

public class CloudberryTLPDistinctOracle extends CloudberryTLPDeduplicationBase {

    public CloudberryTLPDistinctOracle(PostgresGlobalState state) {
        super(state);
    }

    @Override
    protected void prepareSelect() {
        select.setSelectOption(SelectType.DISTINCT);
        select.setFetchColumns(CloudberryOracleUtils.randomFetchColumns(targetTables.getColumns()));
    }

}
