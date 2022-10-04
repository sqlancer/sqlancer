package sqlancer.yugabyte.ysql.gen;

import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.yugabyte.ysql.YSQLGlobalState;

public final class YSQLVacuumGenerator {

    private YSQLVacuumGenerator() {
    }

    public static SQLQueryAdapter create(YSQLGlobalState globalState) {
        String sb = "VACUUM";
        ExpectedErrors errors = new ExpectedErrors();
        errors.add("VACUUM cannot run inside a transaction block");
        return new SQLQueryAdapter(sb, errors);
    }

}
