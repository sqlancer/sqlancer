package sqlancer.mysql.gen;

import java.util.Arrays;

import sqlancer.IgnoreMeException;
import sqlancer.Query;
import sqlancer.QueryAdapter;
import sqlancer.Randomly;
import sqlancer.mysql.MySQLGlobalState;
import sqlancer.mysql.MySQLSchema.MySQLTable;

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

	public static Query generate(MySQLGlobalState globalState) {
		MySQLTable table = globalState.getSchema().getRandomTable();
		if (!table.hasIndexes()) {
			throw new IgnoreMeException();
		}
		StringBuilder sb = new StringBuilder();
		sb.append("DROP INDEX ");
		sb.append(table.getRandomIndex().getIndexName());
		sb.append(" ON ");
		sb.append(table.getName());
		if (Randomly.getBoolean()) {
			sb.append(" ALGORITHM=");
			sb.append(Randomly.fromOptions("DEFAULT", "INPLACE", "COPY"));
		}
		if (Randomly.getBoolean()) {
			sb.append(" LOCK=");
			sb.append(Randomly.fromOptions("DEFAULT", "NONE", "SHARED", "EXCLUSIVE"));
		}
		return new QueryAdapter(sb.toString(),
				Arrays.asList("LOCK=NONE is not supported", "ALGORITHM=INPLACE is not supported", "Data truncation", "Data truncated for functional index"));
	}

}
