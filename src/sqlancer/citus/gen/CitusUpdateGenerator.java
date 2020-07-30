package sqlancer.citus.gen;

import java.util.Set;

import sqlancer.Query;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.gen.PostgresUpdateGenerator;

public class CitusUpdateGenerator extends PostgresUpdateGenerator {

    public static Query create(PostgresGlobalState globalState) {
        Query updateQuery = PostgresUpdateGenerator.create(globalState);
        Set<String> errors = (Set<String>) updateQuery.getExpectedErrors();
        CitusCommon.addCitusErrors(errors);
        return updateQuery;
    }

}
