package sqlancer.cloudberry.gen;

import sqlancer.cloudberry.CloudberryBugs;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.gen.PostgresSetGenerator;

public final class CloudberrySetGenerator {

    private CloudberrySetGenerator() {
    }

    public static SQLQueryAdapter create(PostgresGlobalState globalState) {
        SQLQueryAdapter query;
        do {
            query = PostgresSetGenerator.create(globalState);
        } while (CloudberryBugs.bug2137 && query.getQueryString().contains("wal_compression"));
        return query;
    }

}
