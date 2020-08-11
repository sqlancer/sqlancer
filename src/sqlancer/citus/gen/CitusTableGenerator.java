package sqlancer.citus.gen;

import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresSchema;
import sqlancer.postgres.gen.PostgresTableGenerator;

public class CitusTableGenerator extends PostgresTableGenerator {

    public CitusTableGenerator(String tableName, PostgresSchema newSchema, boolean generateOnlyKnown,
            PostgresGlobalState globalState) {
        super(tableName, newSchema, generateOnlyKnown, globalState);
        CitusCommon.addCitusErrors(errors);
    }

}
