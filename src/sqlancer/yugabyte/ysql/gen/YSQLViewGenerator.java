package sqlancer.yugabyte.ysql.gen;

import sqlancer.Randomly;
import sqlancer.common.DBMSCommon;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.yugabyte.ysql.YSQLErrors;
import sqlancer.yugabyte.ysql.YSQLGlobalState;
import sqlancer.yugabyte.ysql.YSQLVisitor;
import sqlancer.yugabyte.ysql.ast.YSQLSelect;

public final class YSQLViewGenerator {

    private YSQLViewGenerator() {
    }

    public static SQLQueryAdapter create(YSQLGlobalState globalState) {
        ExpectedErrors errors = new ExpectedErrors();
        StringBuilder sb = new StringBuilder("CREATE");
        if (Randomly.getBoolean()) {
            sb.append(" MATERIALIZED");
        } else {
            if (Randomly.getBoolean()) {
                sb.append(" OR REPLACE");
            }
            if (Randomly.getBoolean()) {
                sb.append(Randomly.fromOptions(" TEMP", " TEMPORARY"));
            }
        }
        sb.append(" VIEW ");
        String name = globalState.getSchema().getFreeViewName();
        sb.append(name);
        sb.append("(");
        int nrColumns = Randomly.smallNumber() + 1;
        for (int i = 0; i < nrColumns; i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append(DBMSCommon.createColumnName(i));
        }
        sb.append(")");
        sb.append(" AS (");
        YSQLSelect select = YSQLRandomQueryGenerator.createRandomQuery(nrColumns, globalState);
        sb.append(YSQLVisitor.asString(select));
        sb.append(")");
        YSQLErrors.addGroupingErrors(errors);
        YSQLErrors.addViewErrors(errors);
        YSQLErrors.addCommonExpressionErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

}
