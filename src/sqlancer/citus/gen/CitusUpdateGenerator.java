package sqlancer.citus.gen;

import sqlancer.ExpectedErrors;
import sqlancer.Query;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.gen.PostgresUpdateGenerator;

public final class CitusUpdateGenerator {

    private CitusUpdateGenerator() {
    }

    public static Query create(PostgresGlobalState globalState) {
        Query updateQuery = PostgresUpdateGenerator.create(globalState);
        ExpectedErrors errors = updateQuery.getExpectedErrors();
        CitusCommon.addCitusErrors(errors);
        return updateQuery;
    }

}
