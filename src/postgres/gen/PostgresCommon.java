package postgres.gen;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.Collectors;

import lama.IgnoreMeException;
import lama.Randomly;
import lama.mysql.MySQLVisitor;
import postgres.PostgresProvider;
import postgres.PostgresSchema;
import postgres.PostgresSchema.PostgresColumn;
import postgres.PostgresSchema.PostgresDataType;
import postgres.PostgresSchema.PostgresTable;
import postgres.PostgresVisitor;

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
				// TODO: support CHAR (without VAR)
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
		CHECK, UNIQUE, PRIMARY_KEY, FOREIGN_KEY, EXCLUDE
	}
	
	private enum StorageParameters {
		FILLFACTOR("fillfactor", (r) -> r.getInteger(10, 100)),
		// toast_tuple_target
		PARALLEL_WORKERS("parallel_workers", (r) -> r.getInteger(0, 1024)),
		AUTOVACUUM_ENABLED("autovacuum_enabled", (r) -> Randomly.fromOptions(0, 1)),
		AUTOVACUUM_VACUUM_THRESHOLD("autovacuum_vacuum_threshold", (r) -> r.getInteger(0, 2147483647)),
		OIDS("oids", (R) -> Randomly.fromOptions(0, 1)),
		AUTOVACUUM_VACUUM_SCALE_FACTOR("autovacuum_vacuum_scale_factor",
				(r) -> Randomly.fromOptions(0, 0.00001, 0.01, 0.1, 0.2, 0.5, 0.8, 0.9, 1)),
		AUTOVACUUM_ANALYZE_THRESHOLD("autovacuum_analyze_threshold", (r) -> r.getLong(0, Integer.MAX_VALUE)),
		AUTOVACUUM_ANALYZE_SCALE_FACTOR("autovacuum_analyze_scale_factor",
				(r) -> Randomly.fromOptions(0, 0.00001, 0.01, 0.1, 0.2, 0.5, 0.8, 0.9, 1)),
		AUTOVACUUM_VACUUM_COST_DELAY("autovacuum_vacuum_cost_delay", (r) -> r.getLong(0, 100)),
		AUTOVACUUM_VACUUM_COST_LIMIT("autovacuum_vacuum_cost_limit", (r) -> r.getLong(1, 10000)),
		AUTOVACUUM_FREEZE_MIN_AGE("autovacuum_freeze_min_age", (r) -> r.getLong(0, 1000000000)),
		AUTOVACUUM_FREEZE_MAX_AGE("autovacuum_freeze_max_age", (r) -> r.getLong(100000, 2000000000)),
		AUTOVACUUM_FREEZE_TABLE_AGE("autovacuum_freeze_table_age", (r) -> r.getLong(0, 2000000000));
		// TODO

		private String parameter;
		private Function<Randomly, Object> op;

		private StorageParameters(String parameter, Function<Randomly, Object> op) {
			this.parameter = parameter;
			this.op = op;
		}
	}
	
	public static void generateWith(StringBuilder sb, Randomly r) {
		if (true) {
			return; // FIXME;
		}
		if (Randomly.getBoolean()) {
			sb.append(" WITH (");
			ArrayList<StorageParameters> values = new ArrayList<>(Arrays.asList(StorageParameters.values()));
			if (PostgresProvider.IS_POSTGRES_TWELVE) {
				values.remove(StorageParameters.OIDS);
			}
			List<StorageParameters> subset = Randomly.nonEmptySubset(values);
			int i = 0;
			for (StorageParameters parameter : subset) {
				if (i++ != 0) {
					sb.append(", ");
				}
				sb.append(parameter.parameter);
				sb.append("=");
				sb.append(parameter.op.apply(r));
			}
			sb.append(")");
		}
	}

	public static void addTableConstraints(boolean excludePrimaryKey, StringBuilder sb, PostgresTable table, Randomly r,
			PostgresSchema schema, List<String> errors) throws AssertionError {
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
			addTableConstraint(sb, table, r, t, schema, errors);
		}
	}

	public static void addTableConstraint(StringBuilder sb, PostgresTable table, Randomly r, PostgresSchema schema, List<String> errors) {
		addTableConstraint(sb, table, r, Randomly.fromOptions(TableConstraints.values()), schema, errors);
	}

	private static void addTableConstraint(StringBuilder sb, PostgresTable table, Randomly r, TableConstraints t,
			PostgresSchema schema,  List<String> errors) throws AssertionError {
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
			appendIndexParameters(sb, r);
			break;
		case PRIMARY_KEY:
			sb.append("PRIMARY KEY(");
			sb.append(randomNonEmptyColumnSubset.stream().map(c -> c.getName()).collect(Collectors.joining(", ")));
			sb.append(")");
			appendIndexParameters(sb, r);
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
		case EXCLUDE:
			sb.append("EXCLUDE ");
			sb.append("(");
			// TODO [USING index_method ]
			for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
				if (i != 0) {
					sb.append(", ");
				}
				appendExcludeElement(sb, r, table.getColumns());
				sb.append(" WITH ");
				appendOperator(sb);
			}
			sb.append(")");
			appendIndexParameters(sb, r);
			errors.add("is not valid");
			errors.add("no operator matches");
			errors.add("operator does not exist");
			errors.add("unknown has no default operator class");
			errors.add("exclusion constraints are not supported on partitioned tables");
			errors.add("The exclusion operator must be related to the index operator class for the constraint");
			errors.add("could not create exclusion constraint");
			// TODO: index parameters
			if (Randomly.getBoolean()) {
				sb.append(" WHERE ");
				sb.append("(");
				sb.append(PostgresVisitor.asString(PostgresExpressionGenerator.generateExpression(r, table.getColumns(), PostgresDataType.BOOLEAN)));
				sb.append(")");
			}
			break;
		default:
			throw new AssertionError(t);
		}
	}

	private static void appendIndexParameters(StringBuilder sb, Randomly r) {
		if (Randomly.getBoolean()) {
			generateWith(sb, r);
		}
		// TODO: [ USING INDEX TABLESPACE tablespace ]
	}

	private static void appendOperator(StringBuilder sb) {
		// TODO operators
		sb.append(Randomly.fromOptions("&&", "+", "=", "!="));
	}

	private static void appendExcludeElement(StringBuilder sb, Randomly r, List<PostgresColumn> columns) {
		if (Randomly.getBoolean()) {
			// append column name
			sb.append(Randomly.fromList(columns).getName());
		} else {
			// append expression
			sb.append("(");
			sb.append(PostgresVisitor.asString(PostgresExpressionGenerator.generateExpression(r, columns)));
			sb.append(")");
		}
		// TODO [opclass]
		if (Randomly.getBoolean()) {
			sb.append(" ");
			sb.append(Randomly.fromOptions("ASC", "DESC"));
		}
		if (Randomly.getBoolean()) {
			sb.append(" NULLS ");
			sb.append(Randomly.fromOptions("FIRST", "LAST"));
		}
	}

	private static void deleteOrUpdateAction(StringBuilder sb) {
		sb.append(Randomly.fromOptions("NO ACTION", "RESTRICT", "CASCADE", "SET NULL", "SET DEFAULT"));
	}

}
