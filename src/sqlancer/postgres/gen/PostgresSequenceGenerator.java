package sqlancer.postgres.gen;

import sqlancer.common.gen.AbstractSequenceGenerator;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.postgres.PostgresGlobalState;

public final class PostgresSequenceGenerator extends AbstractSequenceGenerator<PostgresGlobalState> {

    private PostgresSequenceGenerator() {
    }

    public static SQLQueryAdapter createSequence(PostgresGlobalState globalState) {
        return AbstractSequenceGenerator.createSequence(globalState);
    }

}
