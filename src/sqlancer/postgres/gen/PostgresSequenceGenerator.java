package sqlancer.postgres.gen;

import sqlancer.Randomly;
import sqlancer.SQLSequenceGenerator;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.yugabyte.ysql.YSQLGlobalState;

public final class PostgresSequenceGenerator extends SQLSequenceGenerator {

    public PostgresSequenceGenerator() {
        super();
    }

    public static SQLQueryAdapter createSequence(YSQLGlobalState globalState) {
        return SQLSequenceGenerator.createSequence(globalState);
    }
}
