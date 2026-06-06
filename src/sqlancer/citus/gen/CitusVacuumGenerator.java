package sqlancer.citus.gen;

import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.gen.PostgresVacuumGenerator;

public final class CitusVacuumGenerator {

    private CitusVacuumGenerator() {
    }

    public static SQLQueryAdapter create(PostgresGlobalState globalState) {
        SQLQueryAdapter vacuumQuery = PostgresVacuumGenerator.create(globalState);
        ExpectedErrors errors = vacuumQuery.getExpectedErrors();
        CitusCommon.addCitusErrors(errors);
        return vacuumQuery;
    }

}
