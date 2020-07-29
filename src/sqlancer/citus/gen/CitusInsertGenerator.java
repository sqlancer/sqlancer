package sqlancer.citus.gen;

import java.util.HashSet;

import sqlancer.Query;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.gen.PostgresInsertGenerator;

public class CitusInsertGenerator extends PostgresInsertGenerator {

    public static Query insert(PostgresGlobalState globalState) {
        Query insertQuery = PostgresInsertGenerator.insert(globalState);
        HashSet<String> errors = (HashSet<String>) insertQuery.getExpectedErrors();
        CitusCommon.addCitusErrors(errors);
        return insertQuery;
    }

}