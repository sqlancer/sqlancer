package sqlancer.timescaledb;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import com.beust.jcommander.Parameter;

import sqlancer.OracleFactory;
import sqlancer.common.oracle.TestOracle;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresOptions;
import sqlancer.postgres.oracle.PostgresPivotedQuerySynthesisOracle;

public class TimescaleDBOptions extends PostgresOptions {
    @Parameter(names = "--timescaledboracle", description = "Specifies which test oracle should be used for TimeScaleDB extension to PostgreSQL")
    public List<TimescaleDBOracleFactory> timescaleDBOracle = Arrays.asList(TimescaleDBOracleFactory.PQS);

    public enum TimescaleDBOracleFactory implements OracleFactory<PostgresGlobalState> {
        PQS {
            @Override
            public TestOracle<PostgresGlobalState> create(PostgresGlobalState globalState) throws SQLException {
                return new PostgresPivotedQuerySynthesisOracle(globalState);
            }
        },
    }
}
