package sqlancer.postgres.gen;

import sqlancer.Randomly;
import sqlancer.common.DBMSCommon;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresVisitor;
import sqlancer.postgres.ast.PostgresSelect;

public final class PostgresViewGenerator {

    private PostgresViewGenerator() {
    }

    public static SQLQueryAdapter create(PostgresGlobalState globalState) {
        ExpectedErrors errors = new ExpectedErrors();
        StringBuilder sb = new StringBuilder("CREATE");
        boolean materialized;
        boolean recursive = false;
        if (Randomly.getBoolean()) {
            sb.append(" MATERIALIZED");
            materialized = true;
        } else {
            if (Randomly.getBoolean()) {
                sb.append(" OR REPLACE");
            }
            if (Randomly.getBoolean()) {
                sb.append(Randomly.fromOptions(" TEMP", " TEMPORARY"));
            }
            if (Randomly.getBoolean()) {
                sb.append(" RECURSIVE");
                recursive = true;
            }
            materialized = false;
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
        // if (Randomly.getBoolean() && false) {
        // sb.append(" WITH(");
        // if (Randomly.getBoolean()) {
        // sb.append(String.format("security_barrier(%s)", Randomly.getBoolean()));
        // } else {
        // sb.append(String.format("check_option(%s)", Randomly.fromOptions("local1", "cascaded")));
        // }
        // sb.append(")");
        // }
        sb.append(" AS (");
        PostgresSelect select = PostgresRandomQueryGenerator.createRandomQuery(nrColumns, globalState);
        sb.append(PostgresVisitor.asString(select));
        sb.append(")");
        if (Randomly.getBoolean() && !materialized && !recursive) {
            sb.append(" WITH ");
            sb.append(Randomly.fromOptions("CASCADED", "LOCAL"));
            sb.append(" CHECK OPTION");
            errors.add("WITH CHECK OPTION is supported only on automatically updatable views");
        }
        PostgresCommon.addGroupingErrors(errors);
        errors.add("already exists");
        errors.add("cannot drop columns from view");
        errors.add("non-integer constant in ORDER BY"); // TODO
        errors.add("for SELECT DISTINCT, ORDER BY expressions must appear in select list"); // TODO
        errors.add("cannot change data type of view column");
        errors.add("specified more than once"); // TODO
        errors.add("materialized views must not use temporary tables or views");
        errors.add("does not have the form non-recursive-term UNION [ALL] recursive-term");
        errors.add("is not a view");
        errors.add("non-integer constant in DISTINCT ON");
        errors.add("SELECT DISTINCT ON expressions must match initial ORDER BY expressions");
        PostgresCommon.addCommonExpressionErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

}
