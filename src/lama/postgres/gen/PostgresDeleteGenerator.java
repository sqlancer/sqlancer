package lama.postgres.gen;

import java.util.ArrayList;
import java.util.List;

import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import lama.postgres.PostgresGlobalState;
import lama.postgres.PostgresSchema.PostgresDataType;
import lama.postgres.PostgresSchema.PostgresTable;
import lama.postgres.PostgresVisitor;

public class PostgresDeleteGenerator {

	public static Query create(PostgresGlobalState globalState) {
		PostgresTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
		List<String> errors = new ArrayList<>();
		errors.add("violates foreign key constraint");
		errors.add("violates not-null constraint");
		errors.add("could not determine which collation to use for string comparison");
		StringBuilder sb = new StringBuilder("DELETE FROM");
		if (Randomly.getBoolean()) {
			sb.append(" ONLY");
		}
		sb.append(" ");
		sb.append(table.getName());
		if (Randomly.getBoolean()) {
			sb.append(" WHERE ");
			sb.append(PostgresVisitor.asString(
					PostgresExpressionGenerator.generateExpression(globalState.getRandomly(), table.getColumns(), PostgresDataType.BOOLEAN)));
		}
		if (Randomly.getBoolean()) {
			sb.append(" RETURNING ");
			sb.append(PostgresVisitor.asString(PostgresExpressionGenerator.generateExpression(globalState.getRandomly(), table.getColumns())));
		}
		PostgresCommon.addCommonExpressionErrors(errors);
		errors.add("out of range");
		errors.add("cannot cast");
		errors.add("invalid input syntax for");
		errors.add("division by zero");
		return new QueryAdapter(sb.toString(), errors);
	}

}
