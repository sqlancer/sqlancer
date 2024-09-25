package sqlancer.h2;

import java.util.Arrays;
import java.util.List;

import com.beust.jcommander.Parameters;

import sqlancer.DBMSSpecificOptions;

@Parameters(commandDescription = "H2")
public class H2Options implements DBMSSpecificOptions<H2OracleFactory> {

    @Override
    public List<H2OracleFactory> getTestOracleFactory() {
        return Arrays.asList(H2OracleFactory.TLP_WHERE);
    }

}
