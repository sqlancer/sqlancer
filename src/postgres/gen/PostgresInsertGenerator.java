package postgres.gen;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.postgresql.util.PSQLException;

import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import postgres.PostgresProvider;
import postgres.PostgresSchema.PostgresColumn;
import postgres.PostgresSchema.PostgresTable;
import postgres.PostgresVisitor;
import postgres.ast.PostgresConstant;

public class PostgresInsertGenerator {

	public static Query insert(PostgresTable table, Randomly r) {
		List<String> errors = new ArrayList<>();
		if (PostgresProvider.IS_POSTGRES_TWELVE) {
			errors.add("cannot insert into column");
		}
		errors.add("violates foreign key constraint");
		errors.add("value too long for type character varying");
		errors.add("conflicting key value violates exclusion constraint");
		StringBuilder sb = new StringBuilder();
		sb.append("INSERT INTO ");
		sb.append(table.getName());
		List<PostgresColumn> columns = table.getRandomNonEmptyColumnSubset();
		sb.append("(");
		sb.append(columns.stream().map(c -> c.getName()).collect(Collectors.joining(", ")));
		sb.append(")");
		if (Randomly.getBoolean()) {
			sb.append(" OVERRIDING");
			sb.append(" ");
			sb.append(Randomly.fromOptions("SYSTEM", "USER"));
			sb.append(" VALUE");
		}
		sb.append(" VALUES");
		int n = Randomly.smallNumber() + 1;
		for (int i = 0; i < n; i++) {
			if (i != 0) {
				sb.append(", ");
			}
			insertRow(r, sb, columns, n == 1);
		}
		if (Randomly.getBoolean()) {
			sb.append(" ON CONFLICT ");
			if (Randomly.getBoolean()) {
				sb.append("(");
				sb.append(table.getRandomColumn().getName());
				sb.append(")");
			}
			sb.append(" DO NOTHING");
		}
		return new QueryAdapter(sb.toString(), errors) {
			public void execute(java.sql.Connection con) throws java.sql.SQLException {
				try {
					super.execute(con);
				} catch (PSQLException e) {
					if (e.getMessage().contains("violates not-null constraint")) {
						// ignore
					} else if (e.getMessage().contains("duplicate key value violates unique constraint")) {

					}

					else if (e.getMessage().contains("identity column defined as GENERATED ALWAYS")) {

					} else if (e.getMessage().contains(
							"there is no unique or exclusion constraint matching the ON CONFLICT specification")) {

					} else if (e.getMessage().contains("out of range")) {

					} else if (e.getMessage().contains("violates check constraint")) {

					} else if (e.getMessage().contains("no partition of relation")) {

					} else if (e.getMessage().contains("invalid input syntax")) {

					} else if (e.getMessage().contains("division by zero")) {
					} else if (e.getMessage().contains("violates foreign key constraint")) {

					}

					else {
						throw e;
					}
				}

			};
		};
	}

	private static void insertRow(Randomly r, StringBuilder sb, List<PostgresColumn> columns, boolean canBeDefault) {
		sb.append("(");
		for (int i = 0; i < columns.size(); i++) {
			if (i != 0) {
				sb.append(", ");
			}
			if (!Randomly.getBooleanWithSmallProbability() || !canBeDefault) {
				PostgresConstant generateConstant;
				do {
					generateConstant = PostgresExpressionGenerator.generateConstant(r, columns.get(i).getColumnType());
					// make it more unlikely that NULL is inserted
				} while (generateConstant.isNull() || Randomly.getBooleanWithSmallProbability());
				sb.append(PostgresVisitor.asString(generateConstant));
			} else {
				sb.append("DEFAULT");
			}
		}
		sb.append(")");
	}

}
