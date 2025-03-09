package sqlancer;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.yugabyte.ysql.YSQLGlobalState;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLTable.TableType;

public abstract class SQLDiscardGenerator {

    protected SQLDiscardGenerator() {
    }

    protected static boolean canDiscardTemporaryTables(String what) {
        return what.contentEquals("TEMPORARY") || what.contentEquals("TEMP") || what.contentEquals("ALL");
    }
}
