package postgres.gen;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.postgresql.util.PSQLException;

import lama.IgnoreMeException;
import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import postgres.PostgresSchema.PostgresColumn;
import postgres.PostgresSchema.PostgresDataType;
import postgres.PostgresSchema.PostgresTable;
import postgres.PostgresVisitor;

public class PostgresAlterTableGenerator {

	private PostgresTable randomTable;
	private Randomly r;
	private boolean changesSchema;
	private static PostgresColumn randomColumn;

	private enum Action {
		// TODO:     ADD [ COLUMN ] column data_type [ COLLATE collation ] [ column_constraint [ ... ] ]
		ALTER_TABLE_DROP_COLUMN, //     DROP [ COLUMN ] [ IF EXISTS ] column [ RESTRICT | CASCADE ]
		ALTER_COLUMN_TYPE, //     ALTER [ COLUMN ] column [ SET DATA ] TYPE data_type [ COLLATE collation ] [ USING expression ]
		ALTER_COLUMN_SET_DROP_DEFAULT, // ALTER [ COLUMN ] column SET DEFAULT expression and ALTER [ COLUMN ] column DROP DEFAULT
		ALTER_COLUMN_SET_DROP_NULL, // ALTER [ COLUMN ] column { SET | DROP } NOT NULL
		ALTER_COLUMN_SET_STATISTICS, // ALTER [ COLUMN ] column SET STATISTICS integer
		ALTER_COLUMN_SET_ATTRIBUTE_OPTION, // ALTER [ COLUMN ] column SET ( attribute_option = value [, ... ] )
		ALTER_COLUMN_RESET_ATTRIBUTE_OPTION, // ALTER [ COLUMN ] column RESET ( attribute_option [, ... ] )
		ALTER_COLUMN_SET_STORAGE, // ALTER [ COLUMN ] column SET STORAGE { PLAIN | EXTERNAL | EXTENDED | MAIN }
		SET_WITHOUT_CLUSTER, //
		SET_WITH_OIDS, //
		SET_WITHOUT_OIDS, //
		NOT_OF
	}

	public PostgresAlterTableGenerator(PostgresTable randomTable, Randomly r) {
		this.randomTable = randomTable;
		this.r = r;
	}

	public static Query create(PostgresTable randomTable, Randomly r) {
		return new PostgresAlterTableGenerator(randomTable, r).generate();
	}

	private enum Attribute {
		N_DISTINCT_INHERITED("n_distinct_inherited"), N_DISTINCT("n_distinct");

		private String val;

		private Attribute(String val) {
			this.val = val;
		}
	};

	public Query generate() {
		List<String> errors = new ArrayList<>();
		errors.add("invalid input syntax for");
		StringBuilder sb = new StringBuilder();
		sb.append("ALTER TABLE ");
		if (Randomly.getBoolean()) {
			sb.append(" ONLY");
		}
		sb.append(" ");
		sb.append(randomTable.getName());
		sb.append(" ");
		int i = 0;
		List<Action> action = Randomly.nonEmptySubset(Action.values());
		if (randomTable.getColumns().size() == 1) {
			action.remove(Action.ALTER_TABLE_DROP_COLUMN);
		}
		if (action.isEmpty()) {
			throw new IgnoreMeException();
		}
		for (Action a : action) {
			if (i++ != 0) {
				sb.append(", ");
			}
			switch (a) {
			case ALTER_TABLE_DROP_COLUMN:
				sb.append("DROP ");
				if (Randomly.getBoolean()) {
					sb.append(" IF EXISTS ");
				}
				sb.append(randomTable.getRandomColumn().getName());
				if (Randomly.getBoolean()) {
					sb.append(" ");
					sb.append(Randomly.fromOptions("RESTRICT", "CASCADE"));
				}
				changesSchema = true;
				errors.add("does not exist");
				errors.add("cannot drop column referenced in partition key expression");
				errors.add("cannot drop column named in partition key");
				break;
			case ALTER_COLUMN_TYPE:
				alterColumn(randomTable, sb);
				if (Randomly.getBoolean()) {
					sb.append(" SET DATA");
				}
				sb.append(" TYPE ");
				PostgresDataType randomType = PostgresDataType.getRandomType();
				PostgresCommon.appendDataType(randomType, sb, false);
				// TODO [ COLLATE collation ] [ USING expression ]
				errors.add("cannot be cast automatically");
				errors.add("is an identity column");
				errors.add("identity column type must be smallint, integer, or bigint");
				errors.add("out of range");
				errors.add("cannot alter type of column named in partition key");
				errors.add("cannot alter type of column referenced in partition key expression");
				errors.add("argument of CHECK must be type boolean");
				errors.add("operator does not exist");
				changesSchema = true;
				break;
			case ALTER_COLUMN_SET_DROP_DEFAULT:
				alterColumn(randomTable, sb);
				if (Randomly.getBoolean()) {
					sb.append("DROP DEFAULT");
				} else {
					sb.append("SET DEFAULT ");
					sb.append(PostgresVisitor
							.asString(PostgresExpressionGenerator.generateExpression(r, randomColumn.getColumnType())));
					errors.add("is out of range");
				}
				errors.add("is an identity column");
				break;
			case ALTER_COLUMN_SET_DROP_NULL:
				alterColumn(randomTable, sb);
				if (Randomly.getBoolean()) {
					sb.append("SET NOT NULL");
					errors.add("contains null values");
				} else {
					sb.append("DROP NOT NULL");
					errors.add("is in a primary key");
					errors.add("is an identity column");
				}
				break;
			case ALTER_COLUMN_SET_STATISTICS:
				alterColumn(randomTable, sb);
				sb.append("SET STATISTICS ");
				sb.append(r.getInteger(0, 10000));
				break;
			case ALTER_COLUMN_SET_ATTRIBUTE_OPTION:
				alterColumn(randomTable, sb);
				sb.append(" SET(");
				List<Attribute> subset = Randomly.nonEmptySubset(Attribute.values());
				int j = 0;
				for (Attribute attr : subset) {
					if (j++ != 0) {
						sb.append(", ");
					}
					sb.append(attr.val);
					sb.append("=");
					sb.append(Randomly.fromOptions(-1, -0.8, -0.5, -0.2, -0.1, -0.00001, -0.0000000001, 0, 0.000000001,
							0.0001, 0.1, 1));
				}
				sb.append(")");
				break;
			case ALTER_COLUMN_RESET_ATTRIBUTE_OPTION:
				alterColumn(randomTable, sb);
				sb.append(" RESET(");
				subset = Randomly.nonEmptySubset(Attribute.values());
				j = 0;
				for (Attribute attr : subset) {
					if (j++ != 0) {
						sb.append(", ");
					}
					sb.append(attr.val);
				}
				sb.append(")");
				break;
			case ALTER_COLUMN_SET_STORAGE:
				alterColumn(randomTable, sb);
				sb.append("SET STORAGE ");
				sb.append(Randomly.fromOptions("PLAIN", "EXTERNAL", "EXTENDED", "MAIN"));
				errors.add("can only have storage");
				errors.add("is an identity column");
				break;
			case SET_WITHOUT_CLUSTER:
				sb.append("SET WITHOUT CLUSTER");
				errors.add("cannot mark index clustered in partitioned table");
				break;
			case SET_WITH_OIDS:
				errors.add("is an identity column");
				sb.append("SET WITH OIDS");
				break;
			case SET_WITHOUT_OIDS:
				sb.append("SET WITHOUT OIDS");
				break;
			case NOT_OF:
				errors.add("is not a typed table");
				sb.append("NOT OF");
				break;
			default:
				throw new AssertionError(a);
			}
		}

		return new QueryAdapter(sb.toString()) {

			@Override
			public void execute(Connection con) throws SQLException {
				try {
					super.execute(con);
				} catch (PSQLException e) {
					boolean found = false;
					for (String error : errors) {
						if (e.getMessage().contains(error)) {
							found = true;
						}
					}
					if (!found) {
						throw e;
					}
				}
			}

			@Override
			public boolean couldAffectSchema() {
				return changesSchema;
			}
		};
	}

	private static void alterColumn(PostgresTable randomTable, StringBuilder sb) {
		sb.append("ALTER ");
		randomColumn = randomTable.getRandomColumn();
		sb.append(randomColumn.getName());
		sb.append(" ");
	}

}
