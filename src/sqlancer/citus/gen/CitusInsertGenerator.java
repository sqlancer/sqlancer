package sqlancer.citus.gen;

import sqlancer.ExpectedErrors;
import sqlancer.Query;
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
