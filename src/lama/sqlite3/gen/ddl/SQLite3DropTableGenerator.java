package lama.sqlite3.gen.ddl;

import java.util.Arrays;

import lama.IgnoreMeException;
import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import lama.sqlite3.schema.SQLite3Schema;

public class SQLite3DropTableGenerator {

	public static Query dropTable(SQLite3Schema newSchema) {
		if (newSchema.getTables(t -> !t.isView()).size() == 1) {
			throw new IgnoreMeException();
		}
		StringBuilder sb = new StringBuilder("DROP TABLE ");
		if (Randomly.getBoolean()) {
			sb.append("IF EXISTS ");
		}
		sb.append(newSchema.getRandomTableOrBailout(t -> !t.isView()).getName());
		return new QueryAdapter(sb.toString(),
				Arrays.asList("[SQLITE_ERROR] SQL error or missing database (foreign key mismatch",
						"Abort due to constraint violation (FOREIGN KEY constraint failed)",
						"SQL error or missing database"),
				true);

	}

}
