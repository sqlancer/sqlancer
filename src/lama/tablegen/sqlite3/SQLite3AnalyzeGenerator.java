package lama.tablegen.sqlite3;

import lama.Query;
import lama.QueryAdapter;

public class SQLite3AnalyzeGenerator {
	
	public static Query generateAnalyze() {
		return new QueryAdapter("ANALYZE");
	}

}
