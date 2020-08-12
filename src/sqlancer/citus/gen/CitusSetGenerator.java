package sqlancer.citus.gen;

import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.Query;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.gen.PostgresSetGenerator;

public final class CitusSetGenerator {

    private CitusSetGenerator() {
    }

    public static Query create(PostgresGlobalState globalState) {
        Query setQuery = PostgresSetGenerator.create(globalState);
        ExpectedErrors errors = setQuery.getExpectedErrors();
        CitusCommon.addCitusErrors(errors);
        return setQuery;
    }

}
