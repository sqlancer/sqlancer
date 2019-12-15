package lama.postgres.gen;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import lama.postgres.PostgresSchema.PostgresColumn;
import lama.postgres.PostgresSchema.PostgresDataType;
import lama.postgres.PostgresSchema.PostgresTable;
import lama.postgres.PostgresVisitor;
import lama.postgres.ast.PostgresExpression;

public class PostgresUpdateGenerator {

	public static Query create(PostgresTable randomTable, Randomly r) {
		StringBuilder sb = new StringBuilder();
		sb.append("UPDATE ");
		sb.append(randomTable.getName());
		sb.append(" SET ");
		List<String> errors = new ArrayList<String>(Arrays.asList("conflicting key value violates exclusion constraint", "reached maximum value of sequence", "violates foreign key constraint", "violates not-null constraint", "violates unique constraint",
				"out of range", "cannot cast", "must be type boolean", "is not unique", " bit string too long", "can only be updated to DEFAULT", "division by zero", "You might need to add explicit type casts.", "invalid regular expression", "View columns that are not columns of their base relation are not updatable"));
		errors.add("multiple assignments to same column"); // view whose columns refer to a column in the referenced table multiple times
		List<PostgresColumn> columns = randomTable.getRandomNonEmptyColumnSubset();
		PostgresCommon.addCommonInsertUpdateErrors(errors);

		for (int i = 0; i < columns.size(); i++) {
			if (i != 0) {
				sb.append(", ");
			}
			PostgresColumn column = columns.get(i);
			sb.append(column.getName());
			sb.append(" = ");
			if (!Randomly.getBoolean()) {
				PostgresExpression constant = PostgresExpressionGenerator.generateConstant(r, column.getColumnType());
				sb.append(PostgresVisitor.asString(constant));
			} else if (Randomly.getBoolean()) {
				sb.append("DEFAULT");
			} else {
				sb.append("(");
				PostgresExpression expr = PostgresExpressionGenerator.generateExpression(r, randomTable.getColumns(),
						column.getColumnType());
				// caused by casts
				sb.append(PostgresVisitor.asString(expr));
				sb.append(")");
			}
		}
		errors.add("invalid input syntax for ");
		errors.add("operator does not exist: text = boolean");
		errors.add("violates check constraint");
		errors.add("could not determine which collation to use for string comparison");
		errors.add("but expression is of type");
		PostgresCommon.addCommonExpressionErrors(errors);
		if (!Randomly.getBooleanWithSmallProbability()) {
			sb.append(" WHERE ");
			PostgresExpression where = PostgresExpressionGenerator.generateExpression(r, randomTable.getColumns(),
					PostgresDataType.BOOLEAN);
			sb.append(PostgresVisitor.asString(where));
		}

	
		return new QueryAdapter(sb.toString(), errors, true);
	}

}
