package sqlancer.citus;

import java.util.Arrays;
import java.util.List;

import com.beust.jcommander.Parameter;

import sqlancer.postgres.PostgresOptions;

public class CitusOptions extends PostgresOptions {

    @Parameter(names = "--repartition", description = "Specifies whether repartition joins should be allowed", arity = 1)
    public boolean repartition = true;

    @Parameter(names = "--citusoracle", description = "Specifies which test oracle should be used for Citus extension to PostgreSQL")
    public List<CitusOracleFactory> citusOracle = Arrays.asList(CitusOracleFactory.QUERY_PARTITIONING);

}
