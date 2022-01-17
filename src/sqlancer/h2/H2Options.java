package sqlancer.h2;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import com.beust.jcommander.Parameters;

import sqlancer.DBMSSpecificOptions;
import sqlancer.OracleFactory;
import sqlancer.common.oracle.TestOracle;
import sqlancer.h2.H2Options.H2OracleFactory;
import sqlancer.h2.H2Provider.H2GlobalState;

@Parameters(commandDescription = "H2")
public class H2Options implements DBMSSpecificOptions<H2OracleFactory> {

    public enum H2OracleFactory implements OracleFactory<H2GlobalState> {

        TLP_WHERE {

            @Override
            public TestOracle create(H2GlobalState globalState) throws SQLException {
                return new H2QueryPartitioningWhereTester(globalState);
            }

        };

    }

    @Override
    public List<H2OracleFactory> getTestOracleFactory() {
        return Arrays.asList(H2OracleFactory.TLP_WHERE);
    }

}
