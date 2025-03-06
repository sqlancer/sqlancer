package sqlancer.yugabyte.ysql.gen;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.yugabyte.ysql.YSQLErrors;
import sqlancer.yugabyte.ysql.YSQLGlobalState;

public final class YSQLMaterializedViewRefresh {

    private YSQLMaterializedViewRefresh() {
    }

    public static SQLQueryAdapter create(YSQLGlobalState globalState) {
        ExpectedErrors errors = new ExpectedErrors();
        StringBuilder sb = new StringBuilder("REFRESH");
        if (Randomly.getBoolean()) {
            sb.append(" MATERIALIZED");
        }
        sb.append(" VIEW ");
        sb.append(globalState.getSchema().getRandomViewOrBailout().getName());
        if (Randomly.getBoolean()) {
            sb.append(" CONCURRENTLY");
        }
        YSQLErrors.addGroupingErrors(errors);
        YSQLErrors.addViewErrors(errors);
        YSQLErrors.addCommonExpressionErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

}
