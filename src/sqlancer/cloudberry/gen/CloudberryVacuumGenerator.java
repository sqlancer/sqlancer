package sqlancer.cloudberry.gen;

import java.util.regex.Pattern;

import sqlancer.cloudberry.CloudberryBugs;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresSchema.PostgresTable;
import sqlancer.postgres.gen.PostgresVacuumGenerator;

public final class CloudberryVacuumGenerator {

    private CloudberryVacuumGenerator() {
    }

    public static SQLQueryAdapter create(PostgresGlobalState globalState) {
        SQLQueryAdapter query;
        do {
            query = PostgresVacuumGenerator.create(globalState);
        } while (CloudberryBugs.bugSharedCatalogDropDatabaseDeadlock && isDatabaseWideVacuumFull(globalState, query));
        return query;
    }

    private static boolean isDatabaseWideVacuumFull(PostgresGlobalState globalState, SQLQueryAdapter query) {
        String queryString = query.getQueryString();
        if (!queryString.contains("FULL")) {
            return false;
        }
        for (PostgresTable table : globalState.getSchema().getDatabaseTables()) {
            if (Pattern.compile("\\b" + table.getName() + "\\b").matcher(queryString).find()) {
                return false;
            }
        }
        return true;
    }

}
