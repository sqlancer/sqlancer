package sqlancer.presto;

import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import sqlancer.SQLOptions;

@Parameters(commandDescription = "Presto")
public class PrestoOptions extends SQLOptions<PrestoOracleFactory> {
    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 8080;

    @Parameter(names = "--oracle")
    public List<PrestoOracleFactory> oracles = List.of(PrestoOracleFactory.NOREC);

    @Parameter(names = "--catalog")
    public String catalog = "memory";

    @Parameter(names = "--schema")
    public String schema = "test";

    @Parameter(names = "--typed-generator", description = "the expression generator type - typed and untyped ")
    public boolean typedGenerator = true;

    @Override
    public List<PrestoOracleFactory> getTestOracleFactory() {
        return oracles;
    }

}
