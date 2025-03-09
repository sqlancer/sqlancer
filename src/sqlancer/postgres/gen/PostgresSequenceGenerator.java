package sqlancer.postgres.gen;

import sqlancer.Randomly;
import sqlancer.SQLSequenceGenerator;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.postgres.PostgresGlobalState;

public final class PostgresSequenceGenerator extends SQLSequenceGenerator {

    private PostgresSequenceGenerator() {
        super();
    }
}
