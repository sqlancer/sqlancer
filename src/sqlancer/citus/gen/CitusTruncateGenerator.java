package sqlancer.citus.gen;

import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.gen.PostgresTruncateGenerator;

public final class CitusTruncateGenerator {

    private CitusTruncateGenerator() {
    }

    public static SQLQueryAdapter create(PostgresGlobalState globalState) {
        SQLQueryAdapter truncateQuery = PostgresTruncateGenerator.create(globalState);
        ExpectedErrors errors = truncateQuery.getExpectedErrors();
        CitusCommon.addCitusErrors(errors);
        return truncateQuery;
    }

}
