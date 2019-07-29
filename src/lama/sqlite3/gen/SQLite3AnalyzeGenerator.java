package lama.sqlite3.gen;

import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import lama.sqlite3.schema.SQLite3Schema;

public class SQLite3AnalyzeGenerator {

	public static Query generateAnalyze(SQLite3Schema newSchema) {
		StringBuilder sb = new StringBuilder("ANALYZE");
		if (Randomly.getBoolean()) {
			sb.append(" ");
			sb.append(newSchema.getRandomTable().getName());
			// TODO index
		}
		return new QueryAdapter(sb.toString());
	}

}
