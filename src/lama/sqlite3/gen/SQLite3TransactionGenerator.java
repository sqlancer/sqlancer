package lama.sqlite3.gen;

import java.sql.Connection;
import java.util.Arrays;

import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import lama.StateToReproduce;

public class SQLite3TransactionGenerator {

	public static Query generateCommit(Connection con, StateToReproduce state) {
		StringBuilder sb = new StringBuilder();
		sb.append(Randomly.fromOptions("COMMIT", "END"));
		if (Randomly.getBoolean()) {
			sb.append(" TRANSACTION");
		}
		return new QueryAdapter(sb.toString(), Arrays.asList("no transaction is active", "FOREIGN KEY constraint failed"), true);
	}

	public static Query generateBeginTransaction(Connection con, StateToReproduce state) {
		StringBuilder sb = new StringBuilder();
		sb.append("BEGIN ");
		if (Randomly.getBoolean()) {
			sb.append(Randomly.fromOptions("DEFERRED", "IMMEDIATE", "EXCLUSIVE"));
		}
		sb.append(" TRANSACTION;");
		return new QueryAdapter(sb.toString(), Arrays.asList("cannot start a transaction within a transaction"));
	}

	public static Query generateRollbackTransaction(Connection con, StateToReproduce state) {
		// TODO: could be extended by savepoint
		return new QueryAdapter("ROLLBACK TRANSACTION;", Arrays.asList("no transaction is active"), true);
	}

}
