package sqlancer.presto;

import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import sqlancer.DBMSSpecificOptions;
import sqlancer.SQLOptions;

@Parameters(commandDescription = "Presto")
public class PrestoOptions extends SQLOptions<PrestoOracleFactory> {

    @Parameter(names = "--oracle")
    public List<PrestoOracleFactory> oracles = List.of(PrestoOracleFactory.NOREC);

    @Override
    public List<PrestoOracleFactory> getTestOracleFactory() {
        return oracles;
    }

}
