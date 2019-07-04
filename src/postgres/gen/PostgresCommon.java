package postgres.gen;

import java.util.List;
import java.util.stream.Collectors;

import lama.Randomly;
import lama.mysql.MySQLVisitor;
import postgres.PostgresSchema.PostgresDataType;
import postgres.PostgresSchema.PostgresTable;

public class PostgresCommon {

	private PostgresCommon() {
	}

	public static boolean appendDataType(PostgresDataType type, StringBuilder sb, boolean allowSerial)
			throws AssertionError {
		boolean serial = false;
		switch (type) {
		case BOOLEAN:
			sb.append("boolean");
			break;
		case INT:
			if (Randomly.getBoolean() && allowSerial) {
				serial = true;
				sb.append(Randomly.fromOptions("serial", "bigserial"));
			} else {
				sb.append(Randomly.fromOptions("smallint", "integer", "bigint"));
			}
			break;
		case TEXT:
			sb.append("TEXT");
			break;
		default:
			throw new AssertionError(type);
		}
		return serial;
	}

	public enum TableConstraints {
		CHECK, UNIQUE, PRIMARY_KEY
	}

	public static void addTableConstraints(boolean excludePrimaryKey, StringBuilder sb, PostgresTable table, Randomly r)
			throws AssertionError {
		List<TableConstraints> tableConstraints = Randomly.nonEmptySubset(TableConstraints.values());
		if (excludePrimaryKey) {
			tableConstraints.remove(TableConstraints.PRIMARY_KEY);
		}
		for (TableConstraints t : tableConstraints) {
			sb.append(", ");
			// TODO add index parameters
			addTableConstraint(sb, table, r, t);
		}
	}

	public static void addTableConstraint(StringBuilder sb, PostgresTable table, Randomly r) {
		addTableConstraint(sb, table, r, Randomly.fromOptions(TableConstraints.values()));
	}
	
	private static void addTableConstraint(StringBuilder sb, PostgresTable table, Randomly r, TableConstraints t)
			throws AssertionError {
		switch (t) {
		case CHECK:
			sb.append("CHECK(");
			sb.append(MySQLVisitor.getExpressionAsString(r, PostgresDataType.BOOLEAN, table.getColumns()));
			sb.append(")");
			break;
		case UNIQUE:
			sb.append("UNIQUE(");
			sb.append(table.getRandomNonEmptyColumnSubset().stream().map(c -> c.getName())
					.collect(Collectors.joining(", ")));
			sb.append(")");
			break;
		case PRIMARY_KEY:
			sb.append("PRIMARY KEY(");
			sb.append(table.getRandomNonEmptyColumnSubset().stream().map(c -> c.getName())
					.collect(Collectors.joining(", ")));
			sb.append(")");
			break;
		default:
			throw new AssertionError(t);
		}
	}

}
