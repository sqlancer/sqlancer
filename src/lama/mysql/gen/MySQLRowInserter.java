package lama.mysql.gen;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import lama.mysql.MySQLSchema.MySQLColumn;
import lama.mysql.MySQLSchema.MySQLDataType;
import lama.mysql.MySQLSchema.MySQLTable;

public class MySQLRowInserter {

	private final MySQLTable table;
	private final Randomly r;
	private final StringBuilder sb = new StringBuilder();
	boolean canFail;

	public MySQLRowInserter(MySQLTable table, Randomly r) {
		this.table = table;
		this.r = r;
	}

	public static Query insertRow(MySQLTable table, Randomly r) throws SQLException {
		if (Randomly.getBoolean()) {
			return new MySQLRowInserter(table, r).generateInsert();
		} else {
			return new MySQLRowInserter(table, r).generateReplace();
		}
	}

	private Query generateReplace() {
		canFail = true;
		sb.append("REPLACE");
		if (Randomly.getBoolean()) {
			sb.append(" ");
			sb.append(Randomly.fromOptions("LOW_PRIORITY", "DELAYED"));
		}
		return generateInto();

	}

	private Query generateInsert() {
		sb.append("INSERT");
		if (Randomly.getBoolean()) {
			sb.append(" ");
			sb.append(Randomly.fromOptions("LOW_PRIORITY", "DELAYED", "HIGH_PRIORITY"));
		}
		if (Randomly.getBoolean()) {
			sb.append(" IGNORE");
		} else {
			canFail = true;
		}
		return generateInto();
	}

	private Query generateInto() {
		sb.append(" INTO ");
		sb.append(table.getName());
		List<MySQLColumn> columns = table.getRandomNonEmptyColumnSubset();
		sb.append("(");
		sb.append(columns.stream().map(c -> c.getName()).collect(Collectors.joining(", ")));
		sb.append(") ");
		sb.append("VALUES");

		int nrRows;
		if (Randomly.getBoolean()) {
			nrRows = 1;
		} else {
			nrRows = 1 + Randomly.smallNumber();
		}
		for (int row = 0; row < nrRows; row++) {
			if (row != 0) {
				sb.append(", ");
			}
			sb.append("(");
			int i = 0;
			for (MySQLColumn c : columns) {
				if (i++ != 0) {
					sb.append(", ");
				}

				if (Randomly.getBooleanWithSmallProbability()) {
					canFail = true;
					sb.append('"');
					sb.append(r.getString());
					sb.append('"');
				} else if (Randomly.getBooleanWithSmallProbability()) {
					sb.append("DEFAULT");
				} else if (Randomly.getBooleanWithSmallProbability()) {
					sb.append("NULL");
				} else {
					// try to insert valid value;
					assert c.getColumnType() == MySQLDataType.INT;
					sb.append(r.getLong((long) -Math.pow(2, c.getPrecision()) - 1,
							(long) Math.pow(2, c.getPrecision() - 1) - 1));
				}

			}
			sb.append(")");
		}
		if (canFail)

		{
			return new QueryAdapter(sb.toString()) {
				public void execute(java.sql.Connection con) throws SQLException {

					try {
						super.execute(con);
					} catch (SQLException e) {
						// IGNORE
					}

				};

			};
		} else {
			return new QueryAdapter(sb.toString()) {
				public void execute(java.sql.Connection con) throws SQLException {
					
					try {
						super.execute(con);
					} catch (SQLException e) {
						if (e.getMessage().contains("Data truncation")) {
							// IGNORE
						}
					}
					
				};
				
			};
		}
	}

}
