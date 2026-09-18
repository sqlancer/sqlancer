package sqlancer.cloudberry.gen;

import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresSchema;
import sqlancer.postgres.gen.PostgresTableGenerator;

/**
 * The PostgreSQL CREATE TABLE generator with Cloudberry's expected errors added. The distribution policy is set
 * afterwards by {@code CloudberryProvider}.
 */
public class CloudberryTableGenerator extends PostgresTableGenerator {

    public CloudberryTableGenerator(String tableName, PostgresSchema newSchema, boolean generateOnlyKnown,
            PostgresGlobalState globalState) {
        super(tableName, newSchema, generateOnlyKnown, globalState);
        CloudberryCommon.addCloudberryErrors(errors);
    }

    public static SQLQueryAdapter generate(String tableName, PostgresSchema newSchema, boolean generateOnlyKnown,
            PostgresGlobalState globalState) {
        return new CloudberryTableGenerator(tableName, newSchema, generateOnlyKnown, globalState).generate();
    }

}
