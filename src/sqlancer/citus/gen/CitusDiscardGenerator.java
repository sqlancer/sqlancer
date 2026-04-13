package sqlancer.citus.gen;

import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.gen.PostgresDiscardGenerator;

public final class CitusDiscardGenerator {

    private CitusDiscardGenerator() {
    }

    public static SQLQueryAdapter create(PostgresGlobalState globalState) {
        SQLQueryAdapter discardQuery = PostgresDiscardGenerator.create(globalState);
        ExpectedErrors errors = discardQuery.getExpectedErrors();
        CitusCommon.addCitusErrors(errors);
        return discardQuery;
    }

}
