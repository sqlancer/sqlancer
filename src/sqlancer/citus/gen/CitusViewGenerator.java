package sqlancer.citus.gen;

import sqlancer.ExpectedErrors;
import sqlancer.Query;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.gen.PostgresViewGenerator;

public final class CitusViewGenerator {

    private CitusViewGenerator() {
    }

    public static Query create(PostgresGlobalState globalState) {
        Query viewQuery = PostgresViewGenerator.create(globalState);
        ExpectedErrors errors = viewQuery.getExpectedErrors();
        CitusCommon.addCitusErrors(errors);
        return viewQuery;
    }

}
