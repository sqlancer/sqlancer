package lama.sqlite3.gen;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import lama.IgnoreMeException;
import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import lama.StateToReproduce.SQLite3StateToReproduce;
import lama.sqlite3.SQLite3Visitor;
import lama.sqlite3.ast.SQLite3SelectStatement;

public class SQLite3ViewGenerator {

	public static Query generate(Connection con, Randomly r, SQLite3StateToReproduce state) throws SQLException {
		StringBuilder sb = new StringBuilder();
		sb.append("CREATE");
		if (Randomly.getBoolean()) {
			sb.append(" ");
			sb.append(Randomly.fromOptions("TEMP", "TEMPORARY"));
		}
		sb.append(" VIEW");
		sb.append(" IF NOT EXISTS ");
		sb.append("v1");
		QueryGenerator queryGen = new QueryGenerator(con, r);
		try {
			SQLite3SelectStatement q = queryGen.getQuery(state);
			int size = q.getFetchColumns().size();
			sb.append("(");
			for (int i = 0; i < size; i++) {
				if (i != 0) {
					sb.append(", ");
				}
				sb.append(SQLite3Common.createColumnName(i));
			}
			sb.append(")");
			sb.append(" AS ");
			sb.append(SQLite3Visitor.asString(q));
			List<String> errors = new ArrayList<>();
			QueryGenerator.addExpectedErrors(errors);
			return new QueryAdapter(sb.toString(), errors) {
				@Override
				public boolean couldAffectSchema() {
					return true;
				}
				
			};
		} catch (AssertionError e) {
			throw new IgnoreMeException();
		}
		
	}
	
}
