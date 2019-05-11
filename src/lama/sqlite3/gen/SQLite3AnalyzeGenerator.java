package lama.sqlite3.gen;

import lama.Query;
import lama.QueryAdapter;

public class SQLite3AnalyzeGenerator {
	
	public static Query generateAnalyze() {
		return new QueryAdapter("ANALYZE");
	}

}
