package lama.mysql.gen;

import java.sql.Connection;
import java.sql.SQLException;

import lama.IgnoreMeException;
import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import lama.mysql.MySQLSchema.MySQLTable;

/**
 * @see https://dev.mysql.com/doc/refman/8.0/en/drop-index.html
 */
public class MySQLDropIndex {

//	DROP INDEX index_name ON tbl_name
//    [algorithm_option | lock_option] ...
//
//algorithm_option:
//    ALGORITHM [=] {DEFAULT|INPLACE|COPY}
//
//lock_option:
//    LOCK [=] {DEFAULT|NONE|SHARED|EXCLUSIVE}

	public static Query generate(MySQLTable randomTable) {
		if (!randomTable.hasIndexes()) {
			throw new IgnoreMeException();
		}
		StringBuilder sb = new StringBuilder();
		sb.append("DROP INDEX ");
		sb.append(randomTable.getRandomIndex().getIndexName());
		sb.append(" ON ");
		sb.append(randomTable.getName());
		if (Randomly.getBoolean()) {
			sb.append(" ALGORITHM=");
			sb.append(Randomly.fromOptions("DEFAULT", "INPLACE", "COPY"));
		}
		if (Randomly.getBoolean()) {
			sb.append(" LOCK=");
			sb.append(Randomly.fromOptions("DEFAULT", "NONE", "SHARED", "EXCLUSIVE"));
		}
		return new QueryAdapter(sb.toString()) {
			@Override
			public void execute(Connection con) throws SQLException {
				try {
					super.execute(con);
				} catch (SQLException e) {
					if (e.getMessage().contains("LOCK=NONE is not supported")) {

					} else if (e.getMessage().contains("ALGORITHM=INPLACE is not supported")) {
						// ignore
					} else if (e.getMessage().contains("Data truncation")) {
						// ignore
					} else {
						throw e;
					}
				}
			}

		};
	}

}
