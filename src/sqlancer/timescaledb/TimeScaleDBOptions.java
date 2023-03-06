package sqlancer.timescaledb;

import com.beust.jcommander.Parameter;
import sqlancer.OracleFactory;
import sqlancer.common.oracle.TestOracle;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresOptions;
import sqlancer.postgres.oracle.PostgresPivotedQuerySynthesisOracle;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

public class TimeScaleDBOptions extends PostgresOptions {
    @Parameter(names = "--timescaledboracle", description = "Specifies which test oracle should be used for TimeScaleDB extension to PostgreSQL")
    public List<TimeScaleDBOracleFactory> timeScaleDBOracle = Arrays.asList(TimeScaleDBOracleFactory.PQS);

    public enum TimeScaleDBOracleFactory implements OracleFactory<PostgresGlobalState> {
        PQS {
            @Override
            public TestOracle<PostgresGlobalState> create(PostgresGlobalState globalState) throws SQLException {
                return new PostgresPivotedQuerySynthesisOracle(globalState);
            }
        },
    }
}
