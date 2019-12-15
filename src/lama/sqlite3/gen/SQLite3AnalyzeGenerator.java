package lama.sqlite3.gen;

import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import lama.sqlite3.schema.SQLite3Schema;

public class SQLite3AnalyzeGenerator {
	
	enum AnalyzeTarget {
		SCHEMA, TABLE, INDEX, SQL_MASTER
	}

	public static Query generateAnalyze(SQLite3Schema newSchema) {
		StringBuilder sb = new StringBuilder("ANALYZE");
		if (Randomly.getBoolean()) {
			sb.append(" ");
			switch (Randomly.fromOptions(AnalyzeTarget.values())) {
			case INDEX:
				sb.append(newSchema.getRandomIndexOrBailout());
				break;
			case SCHEMA:
				sb.append(Randomly.fromOptions("main", "temp"));
				break;
			case SQL_MASTER:
				sb.append("sqlite_master");
				break;
			case TABLE:
				sb.append(newSchema.getRandomTableOrBailout().getName());
				break;
			default:
				throw new AssertionError();
			}
		}
		return new QueryAdapter(sb.toString());
	}

}
