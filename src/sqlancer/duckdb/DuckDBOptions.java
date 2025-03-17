package sqlancer.duckdb;

import java.util.Arrays;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import sqlancer.SQLOptions;

@Parameters(commandDescription = "DuckDB")
public class DuckDBOptions extends SQLOptions<DuckDBOracleFactory> {

    @Parameter(names = "--oracle")
    public List<DuckDBOracleFactory> oracles = Arrays.asList(DuckDBOracleFactory.QUERY_PARTITIONING);

    @Override
    public List<DuckDBOracleFactory> getTestOracleFactory() {
        return oracles;
    }

}
