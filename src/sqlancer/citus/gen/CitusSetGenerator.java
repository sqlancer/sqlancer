package sqlancer.citus.gen;

import java.util.Set;

import sqlancer.Query;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.gen.PostgresSetGenerator;

public class CitusSetGenerator extends PostgresSetGenerator {

    public static Query create(PostgresGlobalState globalState) {
        Query setQuery = PostgresSetGenerator.create(globalState);
        Set<String> errors = (Set<String>) setQuery.getExpectedErrors();
        CitusCommon.addCitusErrors(errors);
        return setQuery;
    }

}
