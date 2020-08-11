package sqlancer.citus.gen;

import java.util.HashSet;

import sqlancer.Query;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.gen.PostgresInsertGenerator;

public final class CitusInsertGenerator {

    private CitusInsertGenerator() {
    }

    public static Query insert(PostgresGlobalState globalState) {
        Query insertQuery = PostgresInsertGenerator.insert(globalState);
        HashSet<String> errors = (HashSet<String>) insertQuery.getExpectedErrors();
        CitusCommon.addCitusErrors(errors);
        return insertQuery;
    }

}
