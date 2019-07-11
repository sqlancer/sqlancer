package postgres.gen;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import lama.IgnoreMeException;
import lama.Randomly;
import lama.mysql.MySQLVisitor;
import postgres.PostgresSchema;
import postgres.PostgresSchema.PostgresColumn;
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
			if (Randomly.getBoolean()) {
				sb.append("TEXT");
			} else {
				// TODO:  support CHAR (without VAR)
				sb.append("VAR");
				sb.append("CHAR");
				sb.append("(");
				sb.append(ThreadLocalRandom.current().nextInt(1, 500));
				sb.append(")");
			}
			break;
		default:
			throw new AssertionError(type);
		}
		return serial;
	}

	public enum TableConstraints {
		CHECK, UNIQUE, PRIMARY_KEY, FOREIGN_KEY
	}

	public static void addTableConstraints(boolean excludePrimaryKey, StringBuilder sb, PostgresTable table, Randomly r,
			PostgresSchema schema) throws AssertionError {
		// TODO constraint name
		List<TableConstraints> tableConstraints = Randomly.nonEmptySubset(TableConstraints.values());
		if (excludePrimaryKey) {
			tableConstraints.remove(TableConstraints.PRIMARY_KEY);
		}
		if (schema.getDatabaseTables().isEmpty()) {
			tableConstraints.remove(TableConstraints.FOREIGN_KEY);
		}
		for (TableConstraints t : tableConstraints) {
			sb.append(", ");
			// TODO add index parameters
			addTableConstraint(sb, table, r, t, schema);
		}
	}

	public static void addTableConstraint(StringBuilder sb, PostgresTable table, Randomly r, PostgresSchema schema) {
		addTableConstraint(sb, table, r, Randomly.fromOptions(TableConstraints.values()), schema);
	}

	private static void addTableConstraint(StringBuilder sb, PostgresTable table, Randomly r, TableConstraints t,
			PostgresSchema schema) throws AssertionError {
		List<PostgresColumn> randomNonEmptyColumnSubset = table.getRandomNonEmptyColumnSubset();
		List<PostgresColumn> otherColumns;
		switch (t) {
		case CHECK:
			sb.append("CHECK(");
			sb.append(MySQLVisitor.getExpressionAsString(r, PostgresDataType.BOOLEAN, table.getColumns()));
			sb.append(")");
			break;
		case UNIQUE:
			sb.append("UNIQUE(");
			sb.append(randomNonEmptyColumnSubset.stream().map(c -> c.getName()).collect(Collectors.joining(", ")));
			sb.append(")");
			break;
		case PRIMARY_KEY:
			sb.append("PRIMARY KEY(");
			sb.append(randomNonEmptyColumnSubset.stream().map(c -> c.getName()).collect(Collectors.joining(", ")));
			sb.append(")");
			break;
		case FOREIGN_KEY:
			sb.append("FOREIGN KEY (");
			sb.append(randomNonEmptyColumnSubset.stream().map(c -> c.getName()).collect(Collectors.joining(", ")));
			sb.append(") REFERENCES ");
			PostgresTable randomOtherTable = schema.getRandomTable();
			sb.append(randomOtherTable.getName());
			if (randomOtherTable.getColumns().size() < randomNonEmptyColumnSubset.size()) {
				throw new IgnoreMeException();
			}
			otherColumns = randomOtherTable.getRandomNonEmptyColumnSubset(randomNonEmptyColumnSubset.size());
			sb.append("(");
			sb.append(otherColumns.stream().map(c -> c.getName()).collect(Collectors.joining(", ")));
			sb.append(")");
			if (Randomly.getBoolean()) {
				sb.append(" ");
				sb.append(Randomly.fromOptions("MATCH FULL", "MATCH SIMPLE"));
			}
			if (Randomly.getBoolean()) {
				sb.append(" ON DELETE ");
				deleteOrUpdateAction(sb);
			}
			if (Randomly.getBoolean()) {
				sb.append(" ON UPDATE ");
				deleteOrUpdateAction(sb);
			}
			if (Randomly.getBoolean()) {
				sb.append(" ");
				if (Randomly.getBoolean()) {
					sb.append("DEFERRABLE");
					if (Randomly.getBoolean()) {
						sb.append(" ");
						sb.append(Randomly.fromOptions("INITIALLY DEFERRED", "INITIALLY IMMEDIATE"));
					}
				} else {
					sb.append("NOT DEFERRABLE");
				}
			}
			break;
		default:
			throw new AssertionError(t);
		}
	}

	private static void deleteOrUpdateAction(StringBuilder sb) {
		sb.append(Randomly.fromOptions("NO ACTION", "RESTRICT", "CASCADE", "SET NULL", "SET DEFAULT"));
	}

}
