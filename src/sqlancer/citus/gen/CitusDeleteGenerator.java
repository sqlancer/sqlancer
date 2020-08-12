package sqlancer.citus.gen;

import sqlancer.ExpectedErrors;
import sqlancer.Query;
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
