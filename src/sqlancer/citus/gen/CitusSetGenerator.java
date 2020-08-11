package sqlancer.citus.gen;

import java.util.Collection;

import sqlancer.Query;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.gen.PostgresSetGenerator;

public final class CitusSetGenerator {

    private CitusSetGenerator() {
    }

    public static Query create(PostgresGlobalState globalState) {
        Query setQuery = PostgresSetGenerator.create(globalState);
        Collection<String> errors = setQuery.getExpectedErrors();
        CitusCommon.addCitusErrors(errors);
        return setQuery;
    }

}
