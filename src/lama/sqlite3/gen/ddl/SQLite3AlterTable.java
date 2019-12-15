package lama.sqlite3.gen.ddl;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import lama.StateToReproduce;
import lama.sqlite3.SQLite3Errors;
import lama.sqlite3.gen.SQLite3ColumnBuilder;
import lama.sqlite3.gen.SQLite3Common;
import lama.sqlite3.schema.SQLite3Schema;
import lama.sqlite3.schema.SQLite3Schema.Column;
import lama.sqlite3.schema.SQLite3Schema.Table;

public class SQLite3AlterTable {

	public static Query alterTable(SQLite3Schema s, Connection con, StateToReproduce state, Randomly r) throws SQLException {
		SQLite3AlterTable alterTable = new SQLite3AlterTable(r);
		return alterTable.getQuery(s, alterTable);
	}

	private final StringBuilder sb = new StringBuilder();
	private final Randomly r;

	private enum Option {
		RENAME_TABLE, RENAME_COLUMN, ADD_COLUMN
	}

	public SQLite3AlterTable(Randomly r) {
		this.r = r;
	}


	private Query getQuery(SQLite3Schema s, SQLite3AlterTable alterTable) throws AssertionError {
		List<String> errors = new ArrayList<>();
		errors.add("error in view");
		errors.add("no such column"); // trigger
		errors.add("error in trigger"); // trigger
		Option option = Randomly.fromOptions(Option.values());
		Table t = s.getRandomTableOrBailout(tab -> !tab.isView() && !tab.isVirtual());
		sb.append("ALTER TABLE ");
		sb.append(t.getName());
		switch (option) {
		case RENAME_TABLE:
			sb.append(" RENAME TO ");
			sb.append(SQLite3Common.getFreeTableName(s));
			break;
		case RENAME_COLUMN:
			Column c = t.getRandomColumn();
			sb.append(" RENAME COLUMN ");
			sb.append(c.getName());
			sb.append(" TO ");
			sb.append(SQLite3Common.getFreeColumnName(t));
			break;
		case ADD_COLUMN:
			sb.append(" ADD COLUMN ");
			String name = SQLite3Common.getFreeColumnName(t);
			// The column may not have a PRIMARY KEY or UNIQUE constraint.
			// The column may not have a default value of CURRENT_TIME, CURRENT_DATE, CURRENT_TIMESTAMP, or an expression in parentheses.
			// If a NOT NULL constraint is specified, then the column must have a default value other than NULL.
			// If foreign key constraints are enabled and a column with a REFERENCES clause is added, the column must have a default value of NULL.
			sb.append(new SQLite3ColumnBuilder().allowPrimaryKey(false).allowUnique(false).allowNotNull(false).allowDefaultValue(false).createColumn(name, r, t.getColumns()));
			errors.add("subqueries prohibited in CHECK constraints");
			errors.add("Cannot add a NOT NULL column with default value NULL");
			break;
		default:
			throw new AssertionError();
		}
		return new QueryAdapter(alterTable.sb.toString(), errors, true);
	}

}
