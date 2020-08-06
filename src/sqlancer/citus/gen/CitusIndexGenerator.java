package sqlancer.citus.gen;

import java.util.HashSet;

import sqlancer.Query;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.gen.PostgresInsertGenerator;

public class CitusIndexGenerator extends PostgresInsertGenerator {

    public static Query generate(PostgresGlobalState globalState) {
        Query createIndexQuery = PostgresInsertGenerator.insert(globalState);
        HashSet<String> errors = (HashSet<String>) createIndexQuery.getExpectedErrors();
        CitusCommon.addCitusErrors(errors);
        return createIndexQuery;
    }
    
}