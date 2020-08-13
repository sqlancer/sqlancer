package sqlancer.citus.gen;

import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.Query;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.gen.PostgresInsertGenerator;

public final class CitusIndexGenerator {

    private CitusIndexGenerator() {
    }

    public static Query generate(PostgresGlobalState globalState) {
        Query createIndexQuery = PostgresInsertGenerator.insert(globalState);
        ExpectedErrors errors = createIndexQuery.getExpectedErrors();
        CitusCommon.addCitusErrors(errors);
        return createIndexQuery;
    }

}
