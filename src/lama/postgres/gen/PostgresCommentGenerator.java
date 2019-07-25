package lama.postgres.gen;

import lama.IgnoreMeException;
import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import lama.postgres.PostgresSchema;
import lama.postgres.PostgresSchema.PostgresTable;

/**
 * @see https://www.postgresql.org/docs/devel/sql-comment.html
 */
public class PostgresCommentGenerator {

	private enum Action {
		INDEX, COLUMN, STATISTICS, TABLE
	}

	public static Query generate(PostgresSchema newSchema, Randomly r) {
		StringBuilder sb = new StringBuilder();
		sb.append("COMMENT ON ");
		Action type = Randomly.fromOptions(Action.values());
		PostgresTable randomTable = newSchema.getRandomTable();
		switch (type) {
		case INDEX:
			sb.append("INDEX ");
			if (randomTable.getIndexes().isEmpty()) {
				throw new IgnoreMeException();
			} else {
				sb.append(randomTable.getRandomIndex().getIndexName());
			}
			break;
		case COLUMN:
			sb.append("COLUMN ");
			sb.append(randomTable.getRandomColumn().getFullQualifiedName());
			break;
		case STATISTICS:
			sb.append("STATISTICS ");
			if (randomTable.getStatistics().isEmpty()) {
				throw new IgnoreMeException();
			} else {
				sb.append(randomTable.getStatistics().get(0).getName());
			}
			break;
		case TABLE:
			sb.append("TABLE ");
			sb.append(randomTable.getName());
			break;
		default:
			throw new AssertionError(type);
		}
		sb.append(" IS ");
		if (Randomly.getBoolean()) {
			sb.append("NULL");
		} else {
			sb.append("'");
			sb.append(r.getString().replace("'", "''"));
			sb.append("'");
		}
		return new QueryAdapter(sb.toString());
	}

}
