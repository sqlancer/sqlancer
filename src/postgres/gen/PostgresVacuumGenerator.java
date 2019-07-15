package postgres.gen;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Collectors;

import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import postgres.PostgresProvider;
import postgres.PostgresSchema.PostgresTable;

public class PostgresVacuumGenerator {

	public static Query create(PostgresTable table) {
		StringBuilder sb = new StringBuilder("VACUUM ");
		if (Randomly.getBoolean()) {
			// VACUUM [ ( { FULL | FREEZE | VERBOSE | ANALYZE | DISABLE_PAGE_SKIPPING } [,
			// ...] ) ] [ table_name [ (column_name [, ...] ) ] ]
			sb.append("(");
			for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
				ArrayList<String> opts = new ArrayList<String>(
						Arrays.asList("FULL", "FREEZE", "ANALYZE", "VERBOSE", "DISABLE_PAGE_SKIPPING"));
				if (PostgresProvider.IS_POSTGRES_TWELVE) {
					opts.add("SKIP_LOCKED");
					opts.add("INDEX_CLEANUP");
					opts.add("TRUNCATE");
				}
				String option = Randomly.fromList(opts);
				if (i != 0) {
					sb.append(", ");
				}
				sb.append(option);
				if (PostgresProvider.IS_POSTGRES_TWELVE && Randomly.getBoolean()) {
					sb.append(" ");
					sb.append(Randomly.fromOptions(1, 0));
				}
			}
			sb.append(")");
			if (Randomly.getBoolean()) {
				addTableAndColumns(table, sb);
			}
		} else {
			String firstOption = Randomly.fromOptions("FULL", "FREEZE", "VERBOSE");
			sb.append(firstOption);
			if (Randomly.getBoolean()) {
				sb.append(" ANALYZE");
				addTableAndColumns(table, sb);
			} else {
				if (Randomly.getBoolean()) {
					sb.append(" ");
					sb.append(table.getName());
				}
			}
		}
		return new QueryAdapter(sb.toString()) {
			@Override
			public void execute(Connection con) throws SQLException {
				try {
					super.execute(con);
				} catch (SQLException e) {
					if (e.getMessage().contains("VACUUM option DISABLE_PAGE_SKIPPING cannot be used with FULL")) {

					} else if (e.getMessage()
							.contains("ERROR: ANALYZE option must be specified when a column list is provided")) {

					} else if (e.getMessage().contains("deadlock")) {
						/*
						 * "FULL" commented out due to
						 * https://www.postgresql.org/message-id/CA%2Bu7OA6pL%
						 * 2B7Xm_NXHLenxffe3tCr3gTamVdr7zPjcWqW0RFM-A%40mail.gmail.com
						 */
					} else if (e.getMessage().contains("VACUUM cannot run inside a transaction block")) {

					} else {
						throw e;
					}
				}
			}
		};
	}

	private static void addTableAndColumns(PostgresTable table, StringBuilder sb) {
		sb.append(" ");
		sb.append(table.getName());
		if (Randomly.getBoolean()) {
			sb.append("(");
			sb.append(table.getRandomNonEmptyColumnSubset().stream().map(c -> c.getName())
					.collect(Collectors.joining(", ")));
			sb.append(")");
		}
	}

}
