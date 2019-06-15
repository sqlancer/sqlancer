package lama.mysql.gen.datadef;

import java.sql.Connection;
import java.sql.SQLException;

import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import lama.mysql.MySQLSchema.MySQLColumn;
import lama.mysql.MySQLSchema.MySQLTable;

public class CreateIndexGenerator {

	private final MySQLTable table;
	private final Randomly r;
	private StringBuilder sb = new StringBuilder();
	private int indexNr;
	private boolean columnIsPrimaryKey;
	private boolean containsInPlace;

	public CreateIndexGenerator(MySQLTable table, Randomly r) {
		this.table = table;
		this.r = r;
	}

	public static Query create(Randomly r, MySQLTable table) {
		return new CreateIndexGenerator(table, r).create();
	}

	public Query create() {

		sb.append("CREATE ");
		if (Randomly.getBoolean()) {
			// "FULLTEXT" TODO Column 'c3' cannot be part of FULLTEXT index
			// A SPATIAL index may only contain a geometrical type column
			sb.append(Randomly.fromOptions("UNIQUE"));
			sb.append(" ");
		}
		sb.append("INDEX i" + indexNr++);
		indexType();
		sb.append(" ON ");
		sb.append(table.getName());
		sb.append("(");
		MySQLColumn randomColumn = table.getRandomColumn();
		columnIsPrimaryKey = randomColumn.isPrimaryKey();
		sb.append(randomColumn.getName());
		sb.append(")");
		indexOption();
		algorithmOption();
		String string = sb.toString();
		sb = new StringBuilder();
		return new QueryAdapter(string) {
			@Override
			public void execute(Connection con) throws SQLException {
				try {
					super.execute(con);
				} catch (java.sql.SQLIntegrityConstraintViolationException e) {
					// IGNORE;
				} catch (SQLException e) {
					if (e.getMessage()
							.startsWith("ALGORITHM=INPLACE is not supported for this operation. Try ALGORITHM=COPY.")
							&& containsInPlace) {
						// ignore
					} else if (e.getMessage().startsWith("A primary key index cannot be invisible")) {
						// ignore
					}

					else {
						throw e;
					}
				}
			}

		};
	}

	private void algorithmOption() {
		if (Randomly.getBoolean()) {
			sb.append(" ALGORITHM");
			if (Randomly.getBoolean()) {
				sb.append("=");
			}
			sb.append(" ");
			String fromOptions = Randomly.fromOptions("DEFAULT", "INPLACE", "COPY");
			if (fromOptions.contentEquals("INPLACE")) {
				containsInPlace = true;
			}
			sb.append(fromOptions);
		}
	}

	private void indexOption() {
		if (Randomly.getBoolean()) {
			sb.append(" ");
			if (columnIsPrimaryKey) {
				// The explicit primary key cannot be made invisible.
				sb.append("VISIBLE");
			} else {
				sb.append(Randomly.fromOptions("VISIBLE", "INVISIBLE"));
			}
		}
	}

	private void indexType() {
		if (Randomly.getBoolean()) {
			sb.append(" USING ");
			sb.append(Randomly.fromOptions("BTREE", "HASH"));
		}
	}
}
