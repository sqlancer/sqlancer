package postgres.gen;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import lama.mysql.MySQLVisitor;
import lama.sqlite3.gen.SQLite3Common;
import postgres.PostgresSchema;
import postgres.PostgresSchema.PostgresColumn;
import postgres.PostgresSchema.PostgresDataType;
import postgres.PostgresSchema.PostgresTable;
import postgres.PostgresVisitor;
import postgres.ast.PostgresExpression;

public class PostgresTableGenerator {

	private String tableName;
	private Randomly r;
	private boolean columnCanHavePrimaryKey;
	private boolean columnHasPrimaryKey;
	private final StringBuilder sb = new StringBuilder();
	private boolean isTemporaryTable;
	private PostgresSchema newSchema;
	private final List<PostgresColumn> columnsToBeAdded = new ArrayList<>();
	private final List<String> errors = new ArrayList<>();
	private final PostgresTable table;

	public PostgresTableGenerator(String tableName, Randomly r, PostgresSchema newSchema) {
		this.tableName = tableName;
		this.r = r;
		this.newSchema = newSchema;
		table = new PostgresTable(tableName, columnsToBeAdded, null, null);
		errors.add("invalid input syntax for");
		errors.add("is not unique");
		errors.add("integer out of range");
		errors.add("division by zero");
	}

	public static Query generate(String tableName, Randomly r, PostgresSchema newSchema) {
		return new PostgresTableGenerator(tableName, r, newSchema).generate();
	}

	private Query generate() {
		columnCanHavePrimaryKey = true;
		sb.append("CREATE");
		if (Randomly.getBoolean()) {
			sb.append(" ");
			isTemporaryTable = true;
			sb.append(Randomly.fromOptions("TEMPORARY", "TEMP"));
		} else if (Randomly.getBoolean()) {
			sb.append(" UNLOGGED");
		}
		sb.append(" TABLE");
		if (Randomly.getBoolean()) {
			sb.append(" IF NOT EXISTS");
		}
		sb.append(" ");
		sb.append(tableName);
		sb.append("(");
		for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
			if (i != 0) {
				sb.append(", ");
			}
			String name = SQLite3Common.createColumnName(i);
			createColumn(name);
		}
		if (Randomly.getBoolean()) {
			errors.add("constraints on temporary tables may reference only temporary tables");
			errors.add("constraints on unlogged tables may reference only permanent or unlogged tables");
			errors.add("constraints on permanent tables may reference only permanent tables");
			errors.add("cannot be implemented");
			errors.add("there is no unique constraint matching given keys for referenced table");
			errors.add("cannot reference partitioned table");
			errors.add("unsupported ON COMMIT and foreign key combination");
			PostgresCommon.addTableConstraints(columnHasPrimaryKey, sb, table, r, newSchema);
		}
		sb.append(")");
		generateInherits();
		generatePartitionBy();
		generateWith();
		if (Randomly.getBoolean() && isTemporaryTable) {
			sb.append(" ON COMMIT ");
			sb.append(Randomly.fromOptions("PRESERVE ROWS", "DELETE ROWS", "DROP"));
			sb.append(" ");
		}
		return new QueryAdapter(sb.toString()) {
			@Override
			public void execute(Connection con) throws SQLException {
				try {
					super.execute(con);
				} catch (SQLException e) {
					if (e.getMessage().contains("not type text")) {

					} else if (e.getMessage().contains("cannot cast")) {

					} else {
						boolean found = false;
						for (String s : errors) {
							if (e.getMessage().contains(s)) {
								found = true;
							}
						}
						if (!found) {
							throw new AssertionError(e);
						}
					}
				}
			}
		};
	}

	private void createColumn(String name) throws AssertionError {
		sb.append(name);
		sb.append(" ");
		PostgresDataType type = PostgresDataType.getRandomType();
		boolean serial = PostgresCommon.appendDataType(type, sb, true);
		PostgresColumn c = new PostgresColumn(name, type);
		c.setTable(table);
		columnsToBeAdded.add(c);
		sb.append(" ");
		if (Randomly.getBoolean()) {
			createColumnConstraint(type, serial);
		}
	}

	private void generatePartitionBy() {
		if (Randomly.getBoolean()) {
			return;
		}
		sb.append(" PARTITION BY ");
		// TODO "RANGE",
		String partitionOption = Randomly.fromOptions("RANGE", "LIST", "HASH");
		sb.append(partitionOption);
		sb.append("(");
		errors.add("cannot use constant expression");
		errors.add("cannot add NO INHERIT constraint to partitioned table");
		errors.add("unrecognized parameter");
		errors.add("unsupported PRIMARY KEY constraint with partition key definition");
		errors.add("which is part of the partition key.");
		errors.add("unsupported UNIQUE constraint with partition key definition");
		int n = partitionOption.contentEquals("LIST") ? 1 : Randomly.smallNumber() + 1;
		for (int i = 0; i < n; i++) {
			if (i != 0) {
				sb.append(", ");
			}
			sb.append("(");
			PostgresExpression expr = PostgresExpressionGenerator.generateExpression(r, columnsToBeAdded);
			sb.append(PostgresVisitor.asString(expr));
			sb.append(")");
		}
		sb.append(")");
	}

	private void generateInherits() {
		if (true) {
			return;
		}
		if (Randomly.getBoolean() && !newSchema.getDatabaseTables().isEmpty()) {
			sb.append(" INHERITS(");
			sb.append(newSchema.getDatabaseTablesRandomSubsetNotEmpty().stream().map(t -> t.getName())
					.collect(Collectors.joining(", ")));
			sb.append(")");
			errors.add("has a type conflict");
		}
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

	private void generateWith() {
		if (Randomly.getBoolean()) {
			sb.append(" WITH (");
			List<StorageParameters> subset = Randomly.nonEmptySubset(StorageParameters.values());
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

	private enum ColumnConstraint {
		NULL_OR_NOT_NULL, UNIQUE, PRIMARY_KEY, DEFAULT, CHECK, GENERATED
	};

	private void createColumnConstraint(PostgresDataType type, boolean serial) {
		List<ColumnConstraint> constraintSubset = Randomly.nonEmptySubset(ColumnConstraint.values());
		if (!columnCanHavePrimaryKey || columnHasPrimaryKey) {
			constraintSubset.remove(ColumnConstraint.PRIMARY_KEY);
		}
		if (constraintSubset.contains(ColumnConstraint.GENERATED)
				&& constraintSubset.contains(ColumnConstraint.DEFAULT)) {
			// otherwise: ERROR: both default and identity specified for column
			constraintSubset.remove(Randomly.fromOptions(ColumnConstraint.GENERATED, ColumnConstraint.DEFAULT));
		}
		if (constraintSubset.contains(ColumnConstraint.GENERATED) && type != PostgresDataType.INT) {
			// otherwise: ERROR: identity column type must be smallint, integer, or bigint
			constraintSubset.remove(ColumnConstraint.GENERATED);
		}
		if (serial) {
			constraintSubset.remove(ColumnConstraint.GENERATED);
			constraintSubset.remove(ColumnConstraint.DEFAULT);
			constraintSubset.remove(ColumnConstraint.NULL_OR_NOT_NULL);

		}
		for (ColumnConstraint c : constraintSubset) {
			sb.append(" ");
			switch (c) {
			case NULL_OR_NOT_NULL:
				sb.append(Randomly.fromOptions("NOT NULL", "NULL"));
				break;
			case UNIQUE:
				sb.append("UNIQUE");
				break;
			case PRIMARY_KEY:
				sb.append("PRIMARY KEY");
				columnHasPrimaryKey = true;
				break;
			case DEFAULT:
				sb.append("DEFAULT");
				sb.append(" (");
				sb.append(MySQLVisitor.getExpressionAsString(r, type));
				sb.append(")");
				// CREATE TEMPORARY TABLE t1(c0 smallint DEFAULT ('566963878'));
				errors.add("out of range");
				break;
			case CHECK:
				sb.append("CHECK (");
				sb.append(MySQLVisitor.getExpressionAsString(r, PostgresDataType.BOOLEAN));
				sb.append(")");
				if (Randomly.getBoolean()) {
					sb.append(" NO INHERIT");
				}
				break;
			case GENERATED:
				sb.append("GENERATED ");
				sb.append(Randomly.fromOptions("ALWAYS", "BY DEFAULT"));
				sb.append(" AS IDENTITY");
				break;
			default:
				throw new AssertionError(sb);
			}
		}
	}

}
