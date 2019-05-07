package lama.tablegen.sqlite3;

import java.sql.Connection;
import java.sql.SQLException;

import lama.Query;
import lama.QueryAdapter;
import lama.Main.StateToReproduce;

public class SQLite3TransactionGenerator {

	public static Query generateCommit(Connection con, StateToReproduce state) {
		return new QueryAdapter("COMMIT;") {
			@Override
			public void execute(Connection con) throws SQLException {
				try {
					super.execute(con);
				} catch (SQLException e) {
					// TODO ignore for now
				}
			}
		};
	}
	
	public static Query generateBeginTransaction(Connection con, StateToReproduce state) {
		return new QueryAdapter("BEGIN TRANSACTION;");
	}

}
