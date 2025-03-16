package sqlancer.postgres.gen;

import sqlancer.SQLSequenceGenerator;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.postgres.PostgresGlobalState;

public final class PostgresSequenceGenerator extends SQLSequenceGenerator {

    private PostgresSequenceGenerator() {
        super();

    }

    public static SQLQueryAdapter createSequence(PostgresGlobalState globalState) {
        return SQLSequenceGenerator.createSequence(globalState);
    }
}
