package lama.sqlite3.gen;

import java.sql.Connection;
import java.sql.SQLException;

import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
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
		StringBuilder sb = new StringBuilder();
		sb.append("BEGIN ");
		sb.append(Randomly.fromOptions("DEFERRED", "IMMEDIATE", "EXCLUSIVE"));
		sb.append(" TRANSACTION;");
		return new QueryAdapter(sb.toString()) {
			@Override
			public void execute(Connection con) throws SQLException {
				try {
					super.execute(con);
				} catch (SQLException e) {
					if (!e.getMessage().contentEquals("[SQLITE_ERROR] SQL error or missing database (cannot start a transaction within a transaction)")) {
						throw e;
					}
				}
			}
		};
	}
	
	public static Query generateRollbackTransaction(Connection con, StateToReproduce state) {
		return new QueryAdapter("ROLLBACK TRANSACTION;") {
			@Override
			public void execute(Connection con) throws SQLException {
				try {
					super.execute(con);
				} catch (SQLException e) {
					if (!e.getMessage().contentEquals("[SQLITE_ERROR] SQL error or missing database (cannot rollback - no transaction is active)")) {
						throw e;
					}
				}
			}
		};
	}


}
