package lama.postgres.gen;

import java.util.ArrayList;
import java.util.List;

import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import lama.postgres.PostgresGlobalState;
import lama.postgres.PostgresVisitor;
import lama.postgres.ast.PostgresSelect;
import lama.sqlite3.gen.SQLite3Common;

public class PostgresViewGenerator {

	public static Query create(PostgresGlobalState globalState) {
		List<String> errors = new ArrayList<>();
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
			sb.append(SQLite3Common.createColumnName(i));
		}
		sb.append(")");
//		if (Randomly.getBoolean() && false) {
//			sb.append(" WITH(");
//			if (Randomly.getBoolean()) {
//				sb.append(String.format("security_barrier(%s)", Randomly.getBoolean()));
//			} else {
//				sb.append(String.format("check_option(%s)", Randomly.fromOptions("local1", "cascaded")));
//			}
//			sb.append(")");
//		}
		sb.append(" AS (");
		PostgresSelect select = PostgresRandomQueryGenerator.createRandomQuery(nrColumns, globalState.getSchema(),
				globalState.getRandomly());
		sb.append(PostgresVisitor.asString(select));
		sb.append(")");
		if (Randomly.getBoolean() && !materialized && !recursive) {
			sb.append(" WITH ");
			sb.append(Randomly.fromOptions("CASCADED", "LOCAL"));
			sb.append(" CHECK OPTION");
			errors.add("WITH CHECK OPTION is supported only on automatically updatable views");
		}

		errors.add("already exists");
		errors.add("cannot drop columns from view");
		errors.add("non-integer constant in GROUP BY"); // TODO
		errors.add("non-integer constant in ORDER BY"); // TODO
		errors.add("for SELECT DISTINCT, ORDER BY expressions must appear in select list"); // TODO
		errors.add("must appear in the GROUP BY clause or be used in an aggregate function"); // TODO
		errors.add("is not in select list");
		errors.add("cannot change data type of view column");
		errors.add("specified more than once"); // TODO
		errors.add("materialized views must not use temporary tables or views");
		errors.add("does not have the form non-recursive-term UNION [ALL] recursive-term");
		errors.add("is not a view");
		errors.add("non-integer constant in DISTINCT ON");
		errors.add("SELECT DISTINCT ON expressions must match initial ORDER BY expressions");
		PostgresCommon.addCommonExpressionErrors(errors);
		return new QueryAdapter(sb.toString(), errors, true);
	}

}
