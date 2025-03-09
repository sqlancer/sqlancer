package sqlancer.yugabyte.ysql.gen;

import sqlancer.Randomly;
import sqlancer.SQLSequenceGenerator;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.yugabyte.ysql.YSQLGlobalState;

public final class YSQLSequenceGenerator extends SQLSequenceGenerator {

    private YSQLSequenceGenerator() {
        super();
    }

    public static SQLQueryAdapter createSequence(YSQLGlobalState globalState) {
        return SQLSequenceGenerator.createSequence(globalState);
    }
}
