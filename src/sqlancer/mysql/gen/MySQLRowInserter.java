package sqlancer.mysql.gen;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Query;
import sqlancer.QueryAdapter;
import sqlancer.Randomly;
import sqlancer.mysql.MySQLGlobalState;
import sqlancer.mysql.MySQLSchema.MySQLColumn;
import sqlancer.mysql.MySQLSchema.MySQLDataType;
import sqlancer.mysql.MySQLSchema.MySQLTable;

public class MySQLRowInserter {

	private final MySQLTable table;
	private final Randomly r;
	private final StringBuilder sb = new StringBuilder();
	boolean canFail;

	public MySQLRowInserter(MySQLTable table, Randomly r) {
		this.table = table;
		this.r = r;
	}

	public static Query insertRow(MySQLGlobalState globalState) throws SQLException {
		MySQLTable table = globalState.getSchema().getRandomTable();
		Randomly r = globalState.getRandomly();
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
				} else if (c.getColumnType() == MySQLDataType.INT) {
					// try to insert valid value;
					long left = (long) -Math.pow(2, c.getPrecision()) - 1;
					long right = (long) Math.pow(2, c.getPrecision() - 1) - 1;
					sb.append(r.getLong(left, right));
				} else {
					sb.append('"');
					sb.append(r.getString());
					sb.append('"');
				}

			}
			sb.append(")");
		}
		if (canFail)

		{
			return new QueryAdapter(sb.toString()); // TODO: specify errors
		} else {
			return new QueryAdapter(sb.toString(), Arrays.asList("Data truncation"));
		}
	}


}
