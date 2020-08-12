package sqlancer.citus.gen;

import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.Query;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.gen.PostgresInsertGenerator;

public final class CitusInsertGenerator {

    private CitusInsertGenerator() {
    }

    public static Query insert(PostgresGlobalState globalState) {
        Query insertQuery = PostgresInsertGenerator.insert(globalState);
        ExpectedErrors errors = insertQuery.getExpectedErrors();
        CitusCommon.addCitusErrors(errors);
        return insertQuery;
    }

}
