package sqlancer.citus.gen;

import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.gen.PostgresInsertGenerator;

public final class CitusIndexGenerator {

    private CitusIndexGenerator() {
    }

    public static SQLQueryAdapter generate(PostgresGlobalState globalState) {
        SQLQueryAdapter createIndexQuery = PostgresInsertGenerator.insert(globalState);
        ExpectedErrors errors = createIndexQuery.getExpectedErrors();
        CitusCommon.addCitusErrors(errors);
        return createIndexQuery;
    }

}
