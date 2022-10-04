package sqlancer.yugabyte.ysql.gen;

import sqlancer.Randomly;
import sqlancer.common.DBMSCommon;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.yugabyte.ysql.YSQLGlobalState;
import sqlancer.yugabyte.ysql.YSQLVisitor;
import sqlancer.yugabyte.ysql.ast.YSQLSelect;

public final class YSQLViewGenerator {

    private YSQLViewGenerator() {
    }

    public static SQLQueryAdapter create(YSQLGlobalState globalState) {
        ExpectedErrors errors = new ExpectedErrors();
        StringBuilder sb = new StringBuilder("CREATE");
        // boolean materialized;
        // boolean recursive = false;
        if (Randomly.getBoolean()) {
            sb.append(" MATERIALIZED");
            // materialized = true;
        } else {
            if (Randomly.getBoolean()) {
                sb.append(" OR REPLACE");
            }
            if (Randomly.getBoolean()) {
                sb.append(Randomly.fromOptions(" TEMP", " TEMPORARY"));
            }
            // if (Randomly.getBoolean()) {
            // sb.append(" RECURSIVE");
            // recursive = true;
            // }
            // materialized = false;
        }
        sb.append(" VIEW ");
        int i = 0;
        String[] name = new String[1];
        while (true) {
            name[0] = "v" + i++;
            if (globalState.getSchema().getDatabaseTables().stream()
                    .noneMatch(tab -> tab.getName().contentEquals(name[0]))) {
                break;
            }
        }
        sb.append(name[0]);
        sb.append("(");
        int nrColumns = Randomly.smallNumber() + 1;
        for (i = 0; i < nrColumns; i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append(DBMSCommon.createColumnName(i));
        }
        sb.append(")");
        // if (Randomly.getBoolean() && !materialized && !recursive) {
        // sb.append(" WITH ");
        // sb.append(Randomly.fromOptions("CASCADED", "LOCAL"));
        // sb.append(" CHECK OPTION");
        // errors.add("WITH CHECK OPTION is supported only on automatically updatable views");
        // }
        sb.append(" AS (");
        YSQLSelect select = YSQLRandomQueryGenerator.createRandomQuery(nrColumns, globalState);
        sb.append(YSQLVisitor.asString(select));
        sb.append(")");
        YSQLCommon.addGroupingErrors(errors);
        YSQLCommon.addViewErrors(errors);
        YSQLCommon.addCommonExpressionErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

}
