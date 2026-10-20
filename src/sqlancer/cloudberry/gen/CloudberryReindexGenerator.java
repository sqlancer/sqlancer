package sqlancer.cloudberry.gen;

import sqlancer.cloudberry.CloudberryBugs;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.gen.PostgresReindexGenerator;

public final class CloudberryReindexGenerator {

    private CloudberryReindexGenerator() {
    }

    public static SQLQueryAdapter create(PostgresGlobalState globalState) {
        SQLQueryAdapter query;
        do {
            query = PostgresReindexGenerator.create(globalState);
        } while (CloudberryBugs.bugSharedCatalogDropDatabaseDeadlock
                && query.getQueryString().startsWith("REINDEX DATABASE ")
                && !query.getQueryString().contains("CONCURRENTLY"));
        return query;
    }

}
