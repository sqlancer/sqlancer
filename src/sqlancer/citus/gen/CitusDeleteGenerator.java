package sqlancer.citus.gen;

import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.Query;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.gen.PostgresDeleteGenerator;

public final class CitusDeleteGenerator {

    private CitusDeleteGenerator() {
    }

    public static Query create(PostgresGlobalState globalState) {
        Query deleteQuery = PostgresDeleteGenerator.create(globalState);
        ExpectedErrors errors = deleteQuery.getExpectedErrors();
        CitusCommon.addCitusErrors(errors);
        return deleteQuery;
    }

}
