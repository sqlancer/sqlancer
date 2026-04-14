package sqlancer.citus.gen;

import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.gen.PostgresReindexGenerator;

public final class CitusReindexGenerator {

    private CitusReindexGenerator() {
    }

    public static SQLQueryAdapter create(PostgresGlobalState globalState) {
        SQLQueryAdapter reindexQuery = PostgresReindexGenerator.create(globalState);
        ExpectedErrors errors = reindexQuery.getExpectedErrors();
        CitusCommon.addCitusErrors(errors);
        return reindexQuery;
    }

}
