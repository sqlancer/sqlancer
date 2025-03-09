package sqlancer.presto;

import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import sqlancer.SQLOptions;

@Parameters(commandDescription = "Presto")
public class PrestoOptions extends SQLOptions<PrestoOracleFactory> {

    @Parameter(names = "--catalog")
    public String catalog = "memory";

    @Parameter(names = "--schema")
    public String schema = "test";

    @Parameter(names = "--typed-generator", description = "the expression generator type - typed and untyped ")
    public boolean typedGenerator = true;

    @Parameter(names = "--oracle")
    public List<PrestoOracleFactory> oracles = List.of(PrestoOracleFactory.NOREC);

    @Override
    public List<PrestoOracleFactory> getTestOracleFactory() {
        return oracles;
    }

}
