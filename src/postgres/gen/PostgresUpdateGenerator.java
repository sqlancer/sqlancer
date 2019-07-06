package postgres.gen;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.postgresql.util.PSQLException;

import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import postgres.PostgresSchema.PostgresColumn;
import postgres.PostgresSchema.PostgresDataType;
import postgres.PostgresSchema.PostgresTable;
import postgres.PostgresVisitor;
import postgres.ast.PostgresConstant;
import postgres.ast.PostgresExpression;

public class PostgresUpdateGenerator {

	public static Query create(PostgresTable randomTable, Randomly r) {
		StringBuilder sb = new StringBuilder();
		sb.append("UPDATE ");
		sb.append(randomTable.getName());
		sb.append(" SET ");
		List<String> errors = new ArrayList(Arrays.asList("reached maximum value of sequence", "violates foreign key constraint", "violates not-null constraint", "violates unique constraint",
				"out of range", "cannot cast", "must be type boolean", "is not unique", "can only be updated to DEFAULT", "division by zero", "You might need to add explicit type casts."));
		List<PostgresColumn> columns = randomTable.getRandomNonEmptyColumnSubset();
		for (int i = 0; i < columns.size(); i++) {
			if (i != 0) {
				sb.append(", ");
			}
			PostgresColumn column = columns.get(i);
			sb.append(column.getName());
			sb.append(" = ");
			if (!Randomly.getBoolean()) {
				PostgresConstant constant = PostgresExpressionGenerator.generateConstant(r, column.getColumnType());
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
		if (!Randomly.getBooleanWithSmallProbability()) {
			sb.append(" WHERE ");
			PostgresExpression where = PostgresExpressionGenerator.generateExpression(r, randomTable.getColumns(),
					PostgresDataType.BOOLEAN);
			sb.append(PostgresVisitor.asString(where));
		}

	
		return new QueryAdapter(sb.toString()) {
			@Override
			public void execute(Connection con) throws SQLException {
				try {
					super.execute(con);
				} catch (PSQLException e) {
					boolean found = false;
					for (String error : errors) {
						if (e.getMessage().contains(error)) {
							found = true;
						}
					}
					if (!found) {
						throw e;
					}
				}
			}
		};
	}

}
