package postgres.gen;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import org.postgresql.util.PSQLException;

import lama.IgnoreMeException;
import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import postgres.PostgresProvider;
import postgres.PostgresSchema;
import postgres.PostgresSchema.PostgresIndex;

public class PostgresReindexGenerator {
	
	private enum Scope {
		INDEX, TABLE, DATABASE;
	}

	public static Query create(PostgresSchema newSchema) {
		StringBuilder sb = new StringBuilder();
		sb.append("REINDEX");
//		if (Randomly.getBoolean()) {
//			sb.append(" VERBOSE");
//		}
		sb.append(" ");
		Scope scope = Randomly.fromOptions(Scope.values());
		switch (scope) {
		case INDEX:
			sb.append("INDEX ");
			if (PostgresProvider.IS_POSTGRES_TWELVE && Randomly.getBoolean()) {
				sb.append("CONCURRENTLY ");
			}
			List<PostgresIndex> indexes = newSchema.getRandomTable().getIndexes();
			if (indexes.isEmpty()) {
				throw new IgnoreMeException();
			}
			sb.append(indexes.stream().map(i -> i.getIndexName()).collect(Collectors.joining()));
			break;
		case TABLE:
			sb.append("TABLE ");
			if (PostgresProvider.IS_POSTGRES_TWELVE && Randomly.getBoolean()) {
				sb.append("CONCURRENTLY ");
			}
			sb.append(newSchema.getRandomTable().getName());
			break;
		case DATABASE:
			sb.append("DATABASE ");
			if (PostgresProvider.IS_POSTGRES_TWELVE && Randomly.getBoolean()) {
				sb.append("CONCURRENTLY ");
			}
			sb.append(newSchema.getDatabaseName());
			break;
		default:
			throw new AssertionError(scope);
		}
		return new QueryAdapter(sb.toString()) {
			@Override
			public void execute(Connection con) throws SQLException {
				try {
					super.execute(con);
				} catch (PSQLException e) {
//					if (e.getMessage().contains("is duplicated")) {
//						// can happen with CREATE INDEX CONCURRENTLY
//					} else {
//						throw e;
//					}
				}
			}
		};
	}

}
