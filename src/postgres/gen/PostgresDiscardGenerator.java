package postgres.gen;

import java.sql.Connection;
import java.sql.SQLException;

import org.postgresql.util.PSQLException;

import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;

public class PostgresDiscardGenerator {

	public static Query create() {
		StringBuilder sb = new StringBuilder();
		sb.append("DISCARD ");
		sb.append(Randomly.fromOptions("ALL", "PLANS", "SEQUENCES", "TEMPORARY", "TEMP"));
		return new QueryAdapter(sb.toString()) {
			@Override
			public void execute(Connection con) throws SQLException {
				try {
					super.execute(con);
				} catch (PSQLException e) {
					if (e.getMessage().contains("cannot run inside a transaction block")) {
						
					} else {
						throw e;
					}
				}
			}
		};
	}

}
