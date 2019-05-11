package lama.sqlite3.gen;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import lama.Main.StateToReproduce;
import lama.sqlite3.schema.SQLite3Schema;
import lama.sqlite3.schema.SQLite3Schema.Table;
import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;

// see https://www.sqlite.org/lang_dropindex.html
public class SQLite3DropIndexGenerator {

	private boolean sureItExists;

	public static Query dropIndex(Connection con, StateToReproduce state, SQLite3Schema s) throws SQLException {
		try (Statement stm = con.createStatement()) {
			SQLite3DropIndexGenerator gen = new SQLite3DropIndexGenerator();
			String query = gen.dropIndex(con, s);
			return new QueryAdapter(query) {
				public void execute(Connection con) throws SQLException {
					try {
						super.execute(con);
					} catch (SQLException e) {
						if (e.getMessage().startsWith(
								"[SQLITE_ERROR] SQL error or missing database (index associated with UNIQUE or PRIMARY KEY constraint cannot be dropped)")) {
							return;
						} else {
							throw new AssertionError(e);
						}
					}
				};
			};
		}
	}

	private String dropIndex(Connection con, SQLite3Schema s) throws SQLException {
		Table randomTable = s.getRandomTable();
		List<String> indices = getIndexes(randomTable, con);
		String indexName = getRandomIndexName(indices);
		StringBuilder sb = new StringBuilder();
		sb.append("DROP INDEX ");
		if (!sureItExists || Randomly.getBoolean()) {
			sb.append("IF EXISTS ");
		}
		sb.append('"');
		sb.append(indexName);
		sb.append('"');
		return sb.toString();
	}

	private enum Options {
		KNOW_INDEX, // query from schema
		INDEX_LIKE, // string that looks like an index
		RANDOM_STRING
	}

	private String getRandomIndexName(List<String> indices) throws AssertionError {
		Options dropOption = Randomly.fromOptions(Options.values());
		switch (dropOption) {
		case KNOW_INDEX:
			if (!indices.isEmpty()) {
				sureItExists = true;
				return Randomly.fromList(indices);
			} else {
				return Randomly.getString();
			}
		case INDEX_LIKE:
			return SQLite3Common.createIndexName(0);
		case RANDOM_STRING:
			return Randomly.getString();
		default:
			throw new AssertionError();
		}
	}

	private static List<String> getIndexes(Table randomTable, Connection con) throws SQLException {
		try (Statement s = con.createStatement()) {
			ResultSet results = s.executeQuery("PRAGMA index_list(" + randomTable.getName() + ")");
			List<String> indices = new ArrayList<>();
			while (results.next()) {
				String index = results.getString(2);
				indices.add(index);
			}
			return indices;
		}
	}

}
