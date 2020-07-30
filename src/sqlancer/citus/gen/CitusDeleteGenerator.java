package sqlancer.citus.gen;

import java.util.Set;

import sqlancer.Query;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.gen.PostgresDeleteGenerator;

public class CitusDeleteGenerator extends PostgresDeleteGenerator {
    
    public static Query create(PostgresGlobalState globalState) {
        Query deleteQuery = PostgresDeleteGenerator.create(globalState);
        Set<String> errors = (Set<String>) deleteQuery.getExpectedErrors();
        CitusCommon.addCitusErrors(errors);
        return deleteQuery;
    }

}