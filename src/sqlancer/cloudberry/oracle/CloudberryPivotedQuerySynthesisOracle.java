package sqlancer.cloudberry.oracle;

import java.sql.SQLException;

import sqlancer.cloudberry.gen.CloudberryCommon;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.oracle.PostgresPivotedQuerySynthesisOracle;

public class CloudberryPivotedQuerySynthesisOracle extends PostgresPivotedQuerySynthesisOracle {

    public CloudberryPivotedQuerySynthesisOracle(PostgresGlobalState globalState) throws SQLException {
        super(globalState);
        CloudberryCommon.addCloudberryErrors(errors);
    }

}
