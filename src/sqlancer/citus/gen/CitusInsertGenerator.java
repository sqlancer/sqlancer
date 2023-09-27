package sqlancer.citus.gen;

import sqlancer.citus.CitusBugs;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.gen.PostgresInsertGenerator;

public final class CitusInsertGenerator {

    private CitusInsertGenerator() {
    }

    public static SQLQueryAdapter insert(PostgresGlobalState globalState) {
        SQLQueryAdapter insertQuery = PostgresInsertGenerator.insert(globalState);
        ExpectedErrors errors = insertQuery.getExpectedErrors();
        CitusCommon.addCitusErrors(errors);
        if (CitusBugs.bug6298) {
            errors.add("columnar_tuple_insert_speculative not implemented");
        }
        return insertQuery;
    }

}
