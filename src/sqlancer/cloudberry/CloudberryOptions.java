package sqlancer.cloudberry;

import java.util.Arrays;
import java.util.List;

import com.beust.jcommander.Parameter;

import sqlancer.postgres.PostgresOptions;

public class CloudberryOptions extends PostgresOptions {

    @Parameter(names = "--cloudberry-oracle", description = "Specifies which test oracle should be used for Apache Cloudberry")
    public List<CloudberryOracleFactory> cloudberryOracle = Arrays.asList(CloudberryOracleFactory.NOREC);

    /**
     * Honors {@code --test-tablespaces} as given. PostgresOptions ORs it with an OS default, so it cannot be turned off
     * on Linux; on an MPP cluster tablespaces also need a directory on every segment.
     */
    @Override
    public boolean isTestTablespaces() {
        return testTablespaces;
    }

}
