package sqlancer.cloudberry.gen;

import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresSchema;
import sqlancer.postgres.gen.PostgresTableGenerator;

/**
 * Reuses the PostgreSQL CREATE TABLE generator verbatim. Storage-format variety (heap / ao_row / ao_column / pax) is
 * obtained for free through the inherited {@code USING <access method>} clause, since Cloudberry registers those access
 * methods in {@code pg_am} and {@link PostgresGlobalState} already reads them. The distribution policy is applied
 * afterwards by {@code CloudberryProvider} via {@code ALTER TABLE ... SET DISTRIBUTED}. This subclass only augments the
 * expected-error set with Cloudberry-specific errors.
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
