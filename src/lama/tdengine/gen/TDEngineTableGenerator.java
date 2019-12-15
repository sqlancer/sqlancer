package lama.tdengine.gen;

import java.util.ArrayList;
import java.util.List;

import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import lama.sqlite3.gen.SQLite3Common;
import lama.tdengine.TDEngineCommon;

public class TDEngineTableGenerator {

	private final StringBuilder sb = new StringBuilder();
	private final String tableName;

	public TDEngineTableGenerator(String tableName) {
		this.tableName = tableName;
	}

	public static Query generate(String tableName) {
		return new TDEngineTableGenerator(tableName).generateQuery();
	}

	private Query generateQuery() {
		List<String> errors = new ArrayList<>();
		errors.add("row length exceeds max length");
		sb.append("CREATE TABLE IF NOT EXISTS " + tableName);
		// TODO keep
		sb.append("(");
		// The first column of each table must be TIMESTAMP type
		sb.append(SQLite3Common.createColumnName(0) + " TIMESTAMP");
		for (int i = 0; i < 1 + Randomly.smallNumber(); i++) {
			sb.append(", ");
			sb.append(SQLite3Common.createColumnName(i+1));
			sb.append(" ");
			sb.append(TDEngineCommon.getRandomTypeString());
		}
		sb.append(")");
		return new QueryAdapter(sb.toString(), errors);
	}

}
