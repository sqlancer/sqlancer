package sqlancer.materialize.gen;

import sqlancer.Randomly;
import sqlancer.common.DBMSCommon;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.materialize.MaterializeGlobalState;
import sqlancer.materialize.MaterializeVisitor;
import sqlancer.materialize.ast.MaterializeSelect;

public final class MaterializeViewGenerator {

    private MaterializeViewGenerator() {
    }

    public static SQLQueryAdapter create(MaterializeGlobalState globalState) {
        ExpectedErrors errors = new ExpectedErrors();
        StringBuilder sb = new StringBuilder("CREATE");
        @SuppressWarnings("unused")
        boolean materialized;
        @SuppressWarnings("unused")
        boolean recursive = false;
        if (Randomly.getBoolean()) {
            sb.append(" MATERIALIZED");
            materialized = true;
        } else {
            if (Randomly.getBoolean()) {
                sb.append(" OR REPLACE");
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
        sb.append(" AS (");
        MaterializeSelect select = MaterializeRandomQueryGenerator.createRandomQuery(nrColumns, globalState);
        sb.append(MaterializeVisitor.asString(select));
        sb.append(")");
        MaterializeCommon.addGroupingErrors(errors);
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
        errors.add("unable to parse column reference in DISTINCT ON clause");
        errors.add("SELECT DISTINCT ON expressions must match initial ORDER BY expressions");
        MaterializeCommon.addCommonExpressionErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

}
