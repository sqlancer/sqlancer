package sqlancer.cloudberry;

import java.util.Arrays;
import java.util.List;

import com.beust.jcommander.Parameter;

import sqlancer.postgres.PostgresOptions;

public class CloudberryOptions extends PostgresOptions {

    @Parameter(names = "--cloudberry-oracle", description = "Specifies which test oracle should be used for Apache Cloudberry")
    public List<CloudberryOracleFactory> cloudberryOracle = Arrays.asList(CloudberryOracleFactory.NOREC);

    /**
     * Honor the {@code --test-tablespaces} flag directly instead of OR-ing it with the OS default. The upstream
     * PostgresOptions returns {@code testTablespaces || osDefault}, which makes {@code --test-tablespaces false} a
     * no-op on Linux and floods the state-generation phase with CREATE TABLESPACE statements. On an MPP cluster
     * tablespaces also need per-segment directories, so testing them is disabled by default for Cloudberry.
     */
    @Override
    public boolean isTestTablespaces() {
        return testTablespaces;
    }

}
