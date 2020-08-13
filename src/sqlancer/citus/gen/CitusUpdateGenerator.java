package sqlancer.citus.gen;

import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.Query;
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
