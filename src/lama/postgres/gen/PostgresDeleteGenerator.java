package lama.postgres.gen;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;

import org.postgresql.util.PSQLException;

import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import lama.postgres.PostgresVisitor;
import lama.postgres.PostgresSchema.PostgresDataType;
import lama.postgres.PostgresSchema.PostgresTable;

public class PostgresDeleteGenerator {

	public static Query create(PostgresTable table, Randomly r) {
		StringBuilder sb = new StringBuilder("DELETE FROM");
		if (Randomly.getBoolean()) {
			sb.append(" ONLY");
		}
		sb.append(" ");
		sb.append(table.getName());
		if (Randomly.getBoolean()) {
			sb.append(" WHERE ");
			sb.append(PostgresVisitor.asString(PostgresExpressionGenerator.generateExpression(r, table.getColumns(), PostgresDataType.BOOLEAN)));
		}
		if (Randomly.getBoolean()) {
			sb.append(" RETURNING ");
			sb.append(PostgresVisitor.asString(PostgresExpressionGenerator.generateExpression(r, table.getColumns())));
		}
		return new QueryAdapter(sb.toString(), Arrays.asList("violates foreign key constraint", "could not determine which collation to use for string comparison")) {
			@Override
			public void execute(Connection con) throws SQLException {
				try {
					super.execute(con);
				} catch (PSQLException e) {
					if (e.getMessage().contains("out of range")) {
						
					} else if (e.getMessage().contains("cannot cast")) {
						
					} else if (e.getMessage().contains("invalid input syntax for")) {
						
					} else if (e.getMessage().contains("division by zero")) {
						
					} else {
						throw e;
					}
				}
			}
		};
	}

}
