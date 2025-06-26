package sqlancer.yugabyte.ysql.gen;

import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.yugabyte.ysql.YSQLErrors;
import sqlancer.yugabyte.ysql.YSQLGlobalState;

public final class YSQLVacuumGenerator {

    private YSQLVacuumGenerator() {
    }

    public static SQLQueryAdapter create(YSQLGlobalState globalState) {
        ExpectedErrors errors = ExpectedErrors.from("VACUUM cannot run inside a transaction block");
        YSQLErrors.addTransactionErrors(errors);
        return new SQLQueryAdapter("VACUUM", errors);
    }

}
