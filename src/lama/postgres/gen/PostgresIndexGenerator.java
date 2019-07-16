package lama.postgres.gen;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.postgresql.util.PSQLException;

import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import lama.postgres.PostgresSchema;
import lama.postgres.PostgresVisitor;
import lama.postgres.PostgresSchema.PostgresColumn;
import lama.postgres.PostgresSchema.PostgresDataType;
import lama.postgres.PostgresSchema.PostgresIndex;
import lama.postgres.PostgresSchema.PostgresTable;
import lama.postgres.ast.PostgresExpression;
import lama.sqlite3.gen.SQLite3Common;

public class PostgresIndexGenerator {

	public static Query generate(PostgresSchema s, Randomly r) {
		StringBuilder sb = new StringBuilder();
		sb.append("CREATE");
		if (Randomly.getBoolean()) {
			sb.append(" UNIQUE");
		}
		sb.append(" INDEX ");
		/*
		 * Commented out as a workaround for
		 * https://www.postgresql.org/message-id/CA%2Bu7OA4XYhc-
		 * qyCgJqwwgMGZDWAyeH821oa5oMzm_HEifZ4BeA%40mail.gmail.com
		 */
//		if (Randomly.getBoolean()) {
//			sb.append("CONCURRENTLY ");
//		}
		PostgresTable randomTable = s.getRandomTable();
		String indexName = getNewIndexName(randomTable);
		sb.append(indexName);
		sb.append(" ON ");
		sb.append(randomTable.getName());
		if (Randomly.getBoolean()) {
			sb.append(" USING ");
			sb.append(Randomly.fromOptions("btree", "hash", "gist", "gin"));
		}

		sb.append("(");
		for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
			if (i != 0) {
				sb.append(", ");
			}
			if (Randomly.getBoolean()) {
				sb.append(randomTable.getRandomColumn().getName());
			} else {
				sb.append("(");
				PostgresExpression expression = PostgresExpressionGenerator.generateExpression(r,
						randomTable.getColumns());
				sb.append(PostgresVisitor.asString(expression));
				sb.append(")");
			}
			if (Randomly.getBoolean()) {
				sb.append(" ");
				sb.append(Randomly.fromOptions("ASC", "DESC"));
			}
			if (Randomly.getBoolean()) {
				sb.append(" NULLS ");
				sb.append(Randomly.fromOptions("FIRST", "LAST"));
			}
		}

		sb.append(")");
		if (Randomly.getBoolean()) {
			sb.append(" INCLUDE(");
			List<PostgresColumn> columns = randomTable.getRandomNonEmptyColumnSubset();
			sb.append(columns.stream().map(c -> c.getName()).collect(Collectors.joining(", ")));
			sb.append(")");
		}
//		if (Randomly.getBoolean()) {
//			sb.append(" WITH ");
//			
//		}
		List<String> errors = new ArrayList<>();
		if (Randomly.getBoolean()) {
			sb.append(" WHERE ");
			PostgresExpression expr = PostgresExpressionGenerator.generateExpression(r, randomTable.getColumns(),
					PostgresDataType.BOOLEAN);
			sb.append(PostgresVisitor.asString(expr));
		}
		errors.add("You might need to add explicit type casts");
		errors.add(" collations are not supported"); // TODO check
		errors.add("because it has pending trigger events");
		return new QueryAdapter(sb.toString(), errors) {
			public void execute(java.sql.Connection con) throws java.sql.SQLException {
				try {
					super.execute(con);
				} catch (PSQLException e) {
					if (e.getMessage().contains("already exists")) {
						return;
					} else if (e.getMessage().contains("could not create unique index")) {

					} else if (e.getMessage().contains("has no default operator class")) {

					} else if (e.getMessage().contains("does not support")) {

					} else if (e.getMessage().contains("cannot cast")) {

					} else if (e.getMessage().contains("unsupported UNIQUE constraint with partition key definition")) {

					} else if (e.getMessage().contains("insufficient columns in UNIQUE constraint definition")) {
						// partition
					} else if (e.getMessage().contains("invalid input syntax for ")) {
						// cast
					} else if (e.getMessage().contains("must be type ")) {
					} else if (e.getMessage().contains("integer out of range")) {
					} else if (e.getMessage().contains("division by zero")) {
					} else if (e.getMessage().contains("out of range")) {
					} else {
						throw e;
					}
				}

			};
		};
	}

	private static String getNewIndexName(PostgresTable randomTable) {
		List<PostgresIndex> indexes = randomTable.getIndexes();
		int indexI = 0;
		while (true) {
			String indexName = SQLite3Common.createIndexName(indexI++);
			if (indexes.stream().noneMatch(i -> i.getIndexName().equals(indexName))) {
				return indexName;
			}
		}
	}

}
