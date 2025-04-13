package sqlancer.hive;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import sqlancer.common.oracle.TLPWhereOracle;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.DBMSSpecificOptions;
import sqlancer.hive.gen.HiveExpressionGenerator;
import sqlancer.OracleFactory;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

@Parameters(separators = "=", commandDescription = "Hive (default port: " + HiveOptions.DEFAULT_PORT
        + ", default host: " + HiveOptions.DEFAULT_HOST + ")")
public class HiveOptions implements DBMSSpecificOptions<HiveOptions.HiveOracleFactory> {
    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 10000;

    @Parameter(names = "--oracle")
    public List<HiveOracleFactory> oracle = Arrays.asList(HiveOracleFactory.TLPWhere);

    public enum HiveOracleFactory implements OracleFactory<HiveGlobalState> {
        TLPWhere {
            @Override
            public TestOracle<HiveGlobalState> create(HiveGlobalState globalState) throws SQLException {
                HiveExpressionGenerator gen = new HiveExpressionGenerator(globalState);
                ExpectedErrors expectedErrors = ExpectedErrors.newErrors()
                        .with(HiveErrors.getExpressionErrors()).build();

                return new TLPWhereOracle<>(globalState, gen, expectedErrors);
            }
        };
    }

    @Override
    public List<HiveOracleFactory> getTestOracleFactory() {
        return oracle;
    }
}
