package sqlancer.citus;

import com.beust.jcommander.Parameter;

import sqlancer.postgres.PostgresOptions;

public class CitusOptions extends PostgresOptions {
    
    @Parameter(names= "--repartition")
    public boolean repartition = true;
    
}
