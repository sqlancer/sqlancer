package sqlancer.yugabyte.ysql.gen;

import java.util.concurrent.atomic.AtomicLong;

import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.yugabyte.ysql.YSQLGlobalState;

public final class YSQLTableGroupGenerator {

    // TODO rework
    public static final AtomicLong UNIQUE_TABLEGROUP_COUNTER = new AtomicLong(1);

    private YSQLTableGroupGenerator() {
    }

    public static SQLQueryAdapter create(YSQLGlobalState globalState) {
        ExpectedErrors errors = new ExpectedErrors();
        StringBuilder sb = new StringBuilder("CREATE TABLEGROUP ");
        String tableGroupName = "tg" + UNIQUE_TABLEGROUP_COUNTER.incrementAndGet();
        sb.append(tableGroupName);
        errors.add("cannot use tablegroups in a colocated database");
        return new SQLQueryAdapter(sb.toString(), errors, true);
    }
}
