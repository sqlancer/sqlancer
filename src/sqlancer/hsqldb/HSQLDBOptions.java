package sqlancer.hsqldb;

import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import sqlancer.DBMSSpecificOptions;

@Parameters(commandDescription = "hsqldb")
public class HSQLDBOptions implements DBMSSpecificOptions<HSQLDBOracleFactory> {

    @Parameter(names = "--oracle")
    public List<HSQLDBOracleFactory> oracle = List.of(HSQLDBOracleFactory.WHERE, HSQLDBOracleFactory.NOREC);

    @Override
    public List<HSQLDBOracleFactory> getTestOracleFactory() {
        return oracle;
    }

}
