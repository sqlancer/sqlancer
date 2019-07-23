package lama.postgres.gen;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import lama.mysql.MySQLVisitor;
import lama.postgres.PostgresProvider;
import lama.postgres.PostgresSchema;
import lama.postgres.PostgresSchema.PostgresColumn;
import lama.postgres.PostgresSchema.PostgresDataType;
import lama.postgres.PostgresSchema.PostgresTable;
import lama.postgres.PostgresVisitor;
import lama.postgres.ast.PostgresExpression;
import lama.sqlite3.gen.SQLite3Common;

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
		table = new PostgresTable(tableName, columnsToBeAdded, null, null, null);
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
			errors.add("ERROR: invalid ON DELETE action for foreign key constraint containing generated column");
			PostgresCommon.addTableConstraints(columnHasPrimaryKey, sb, table, r, newSchema, errors);
		}
		sb.append(")");
		generateInherits();
		generatePartitionBy();
		PostgresCommon.generateWith(sb, r, errors);
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
		if (Randomly.getBoolean() && !newSchema.getDatabaseTables().isEmpty()) {
			sb.append(" INHERITS(");
			sb.append(newSchema.getDatabaseTablesRandomSubsetNotEmpty().stream().map(t -> t.getName())
					.collect(Collectors.joining(", ")));
			sb.append(")");
			errors.add("has a type conflict");
			errors.add("cannot create partitioned table as inheritance child");
			errors.add("cannot inherit from temporary relation");
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
				errors.add("is a generated column");
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
				if (Randomly.getBoolean() && PostgresProvider.IS_POSTGRES_TWELVE) {
					sb.append(" ALWAYS AS (");
					sb.append(PostgresVisitor.asString(PostgresExpressionGenerator.generateExpression(r, columnsToBeAdded, type)));
					sb.append(") STORED");
					errors.add("A generated column cannot reference another generated column.");
					errors.add("cannot use generated column in partition key");
					errors.add("generation expression is not immutable");
				} else {
					sb.append(Randomly.fromOptions("ALWAYS", "BY DEFAULT"));
					sb.append(" AS IDENTITY");
				}
				break;
			default:
				throw new AssertionError(sb);
			}
		}
	}

}
