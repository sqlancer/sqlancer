package sqlancer.citus.gen;

import java.util.Set;

import sqlancer.Query;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.gen.PostgresDeleteGenerator;

public final class CitusDeleteGenerator {

    private CitusDeleteGenerator() {
    }

    public static Query create(PostgresGlobalState globalState) {
        Query deleteQuery = PostgresDeleteGenerator.create(globalState);
        Set<String> errors = (Set<String>) deleteQuery.getExpectedErrors();
        CitusCommon.addCitusErrors(errors);
        return deleteQuery;
    }

}
