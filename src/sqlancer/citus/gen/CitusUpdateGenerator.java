package sqlancer.citus.gen;

import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.gen.PostgresUpdateGenerator;

public final class CitusUpdateGenerator {

    private CitusUpdateGenerator() {
    }

    public static SQLQueryAdapter create(PostgresGlobalState globalState) {
        SQLQueryAdapter updateQuery = PostgresUpdateGenerator.create(globalState);
        ExpectedErrors errors = updateQuery.getExpectedErrors();
        CitusCommon.addCitusErrors(errors);
        return updateQuery;
    }

}
