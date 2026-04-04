package sqlancer.spark;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import sqlancer.DBMSSpecificOptions;
import sqlancer.OracleFactory;
import sqlancer.common.oracle.TLPWhereOracle;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.spark.gen.SparkExpressionGenerator;

@Parameters(separators = "=", commandDescription = "Spark SQL (default port: " + SparkOptions.DEFAULT_PORT
        + ", default host: " + SparkOptions.DEFAULT_HOST + ")")
public class SparkOptions implements DBMSSpecificOptions<SparkOptions.SparkOracleFactory> {
    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 10000;

    @Parameter(names = "--oracle")
    public List<SparkOracleFactory> oracle = Arrays.asList(SparkOracleFactory.TLPWhere);

    public enum SparkOracleFactory implements OracleFactory<SparkGlobalState> {
        TLPWhere {
            @Override
            public TestOracle<SparkGlobalState> create(SparkGlobalState globalState) throws SQLException {
                SparkExpressionGenerator gen = new SparkExpressionGenerator(globalState);
                ExpectedErrors expectedErrors = ExpectedErrors.newErrors().with(SparkErrors.getExpressionErrors())
                        .build();

                return new TLPWhereOracle<>(globalState, gen, expectedErrors);
            }
        };
    }

    @Override
    public List<SparkOracleFactory> getTestOracleFactory() {
        return oracle;
    }
}
