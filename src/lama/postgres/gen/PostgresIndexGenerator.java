package lama.postgres.gen;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import lama.postgres.PostgresGlobalState;
import lama.postgres.PostgresSchema;
import lama.postgres.PostgresSchema.PostgresColumn;
import lama.postgres.PostgresSchema.PostgresDataType;
import lama.postgres.PostgresSchema.PostgresIndex;
import lama.postgres.PostgresSchema.PostgresTable;
import lama.postgres.PostgresVisitor;
import lama.postgres.ast.PostgresExpression;
import lama.sqlite3.gen.SQLite3Common;

public class PostgresIndexGenerator {

	public static enum IndexType {
		BTREE, HASH, GIST, GIN
	}

	public static Query generate(PostgresSchema s, Randomly r, PostgresGlobalState globalState) {
		List<String> errors = new ArrayList<>();
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
		PostgresTable randomTable = s.getRandomTable(t -> !t.isView()); // TODO: materialized views
		String indexName = getNewIndexName(randomTable);
		sb.append(indexName);
		sb.append(" ON ");
		if (Randomly.getBoolean()){
			sb.append("ONLY ");
		}
		sb.append(randomTable.getName());
		IndexType method;
		if (Randomly.getBoolean()) {
			sb.append(" USING ");
			method = Randomly.fromOptions(IndexType.values());
			sb.append(method);
		} else {
			method = IndexType.BTREE;
		}

		sb.append("(");
		if (method == IndexType.HASH) {
			sb.append(randomTable.getRandomColumn().getName());
		} else {
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

//				if (Randomly.getBoolean()) {
//					sb.append(" ");
//					sb.append("COLLATE ");
//					sb.append(Randomly.fromOptions("C", "POSIX"));
//				}
				if (Randomly.getBoolean()) {
					sb.append(" ");
					sb.append(globalState.getRandomOpclass());
					errors.add("does not accept");
					errors.add("does not exist for access method");
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
		}

		sb.append(")");
		if (Randomly.getBoolean() && method != IndexType.HASH) {
			sb.append(" INCLUDE(");
			List<PostgresColumn> columns = randomTable.getRandomNonEmptyColumnSubset();
			sb.append(columns.stream().map(c -> c.getName()).collect(Collectors.joining(", ")));
			sb.append(")");
		}
//		if (Randomly.getBoolean()) {
//			sb.append(" WITH ");
//			
//		}
		if (Randomly.getBoolean()) {
			sb.append(" WHERE ");
			PostgresExpression expr = new PostgresExpressionGenerator(r).setColumns(randomTable.getColumns()).setGlobalState(globalState).generateExpression(PostgresDataType.BOOLEAN);
			sb.append(PostgresVisitor.asString(expr));
		}
		errors.add("already contains data"); // CONCURRENT INDEX failed
		errors.add("You might need to add explicit type casts");
		errors.add(" collations are not supported"); // TODO check
		errors.add("because it has pending trigger events");
		errors.add("could not determine which collation to use for index expression");
		errors.add("could not determine which collation to use for string comparison");
		errors.add("is duplicated");
		errors.add("access method \"gin\" does not support unique indexes");
		errors.add("access method \"hash\" does not support unique indexes");
		errors.add("already exists");
		errors.add("could not create unique index");
		errors.add("has no default operator class");
		errors.add("does not support");
		errors.add("cannot cast");
		errors.add("unsupported UNIQUE constraint with partition key definition");
		errors.add("insufficient columns in UNIQUE constraint definition");
		errors.add("invalid input syntax for");
		errors.add("must be type ");
		errors.add("integer out of range");
		errors.add("division by zero");
		errors.add("out of range");
		errors.add("functions in index predicate must be marked IMMUTABLE");
		errors.add("functions in index expression must be marked IMMUTABLE");
		errors.add("result of range difference would not be contiguous");
		PostgresCommon.addCommonExpressionErrors(errors);
		return new QueryAdapter(sb.toString(), errors);
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
