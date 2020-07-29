package sqlancer.citus.gen;

import java.util.Set;

import sqlancer.Query;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.gen.PostgresViewGenerator;

public class CitusViewGenerator extends PostgresViewGenerator {

    public static Query create(PostgresGlobalState globalState) {
        Query viewQuery = PostgresViewGenerator.create(globalState);
        Set<String> errors = (Set<String>) viewQuery.getExpectedErrors();
        CitusCommon.addCitusErrors(errors);
        return viewQuery;
    }
    
}