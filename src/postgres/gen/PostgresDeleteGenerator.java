package postgres.gen;

import java.sql.Connection;
import java.sql.SQLException;

import org.postgresql.util.PSQLException;

import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import postgres.PostgresSchema.PostgresDataType;
import postgres.PostgresSchema.PostgresTable;
import postgres.PostgresVisitor;

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
		return new QueryAdapter(sb.toString()) {
			@Override
			public void execute(Connection con) throws SQLException {
				try {
					super.execute(con);
				} catch (PSQLException e) {
					if (e.getMessage().contains("out of range")) {
						
					} else if (e.getMessage().contains("cannot cast")) {
						
					} else if (e.getMessage().contains("invalid input syntax for")) {
						
					} else {
						throw e;
					}
				}
			}
		};
	}

}
