package lama.sqlite3.gen;

import java.sql.Connection;
import java.sql.SQLException;

import lama.Main.StateToReproduce;
import lama.QueryAdapter;
import lama.Randomly;

public class SQLite3PragmaGenerator {

	private enum Pragma {
		APPLICATION_ID, AUTO_VACUUM, AUTOMATIC_INDEX, BUSY_TIMEOUT, CACHE_SIZE, CACHE_SPILL_ENABLED, CACHE_SPILL_SIZE,
		// CASE_SENSITIVE_LIKE, // see
		// https://www.mail-archive.com/sqlite-users@mailinglists.sqlite.org/msg115030.html
		CELL_SIZE_CHECK, CHECKPOINT_FULLSYNC, DEFER_FOREIGN_KEY, /*ENCODING,*/ FOREIGN_KEYS, IGNORE_CHECK_CONSTRAINTS, INCREMENTAL_VACUUM, INTEGRITY_CHECK, JOURNAL_MODE, JOURNAL_SIZE_LIMIT, /* LEGACY_ALTER_TABLE ,*/ OPTIMIZE,
		LEGACY_FORMAT, REVERSE_UNORDERED_SELECTS, SECURE_DELETE, SHRINK_MEMORY, SOFT_HEAP_LIMIT, THREADS
	}

	public static QueryAdapter insertPragma(Connection con, StateToReproduce state, Randomly r)
			throws SQLException {
		Pragma p = Randomly.fromOptions(Pragma.values());
		StringBuilder sb = new StringBuilder();
		switch (p) {
		case APPLICATION_ID:
			sb.append("PRAGMA main.application_id=");
			sb.append(r.getInteger());
			break;
		case AUTO_VACUUM:
			sb.append("PRAGMA main.auto_vacuum=");
			sb.append(Randomly.fromOptions("NONE", "FULL", "INCREMENTAL"));
			break;
		case AUTOMATIC_INDEX:
			sb.append("PRAGMA automatic_index = ");
			sb.append(getRandomTextBoolean());
			break;
		case BUSY_TIMEOUT:
			sb.append("PRAGMA busy_timeout = ");
			if (Randomly.getBoolean()) {
				sb.append(0);
			} else {
				long value = Math.max(10000, r.getInteger());
				sb.append(value);
			}
			break;
		case CACHE_SIZE:
			sb.append("PRAGMA main.cache_size=");
			if (Randomly.getBoolean()) {
				sb.append(0);
			} else {
				sb.append(r.getInteger());
			}
			break;
		case CACHE_SPILL_ENABLED:
			sb.append("PRAGMA cache_spill=");
			sb.append(getRandomTextBoolean());
			break;
		case CACHE_SPILL_SIZE:
			sb.append("PRAGMA main.cache_spill=");
			sb.append(r.getInteger());
			break;
//			case CASE_SENSITIVE_LIKE:
//				if (afterIndicesCreated) {
//					sb.append("PRAGMA case_sensitive_like=");
//					sb.append(r.fromOptions("true", "false"));
//					break;
//				} else {
//					continue;
//				}
		case CELL_SIZE_CHECK:
			sb.append("PRAGMA cell_size_check=");
			sb.append(getRandomTextBoolean());
			break;
		case CHECKPOINT_FULLSYNC:
			sb.append("PRAGMA checkpoint_fullfsync=");
			sb.append(getRandomTextBoolean());
			break;
		case DEFER_FOREIGN_KEY:
			sb.append("PRAGMA defer_foreign_keys =");
			sb.append(getRandomTextBoolean());
			break;
			// TODO: [SQLITE_ERROR] SQL error or missing database (attached databases must use the same text encoding as main database)
//		case ENCODING:
//			sb.append("PRAGMA encoding = \"");
//			sb.append(r.fromOptions("UTF-8", "UTF-16", "UTF-16be", "UTF-16le"));
//			sb.append("\"");
//			break;
		case FOREIGN_KEYS:
			sb.append("PRAGMA foreign_keys=");
			sb.append(getRandomTextBoolean());
			break;
		case IGNORE_CHECK_CONSTRAINTS:
			sb.append("PRAGMA ignore_check_constraints=");
			sb.append(getRandomTextBoolean());
			break;
		case INCREMENTAL_VACUUM:
			if (Randomly.getBoolean()) {
				sb.append("PRAGMA incremental_vacuum");
			} else {
				sb.append(String.format("PRAGMA incremental_vacuum(%d)", r.getInteger()));
			}
			break;
		case INTEGRITY_CHECK:
			if (Randomly.getBoolean()) {
				sb.append("PRAGMA integrity_check");
			} else {
				sb.append(String.format("PRAGMA integrity_check(%d)", r.getInteger()));
			}
			break;
		case JOURNAL_MODE:
			sb.append("PRAGMA main.journal_mode=");
			// OFF is no longer generated, since it might corrupt the database upon failed index creation, see https://www.sqlite.org/src/tktview?name=f4ec250930.
			sb.append(Randomly.fromOptions("DELETE", "TRUNCATE", "PERSIST", "MEMORY", "WAL"));
			break;
		case JOURNAL_SIZE_LIMIT:
			sb.append("PRAGMA main.journal_size_limit=");
			if (Randomly.getBoolean()) {
				sb.append(0);
			} else {
				sb.append(r.getInteger());
			}
			break;
//		case LEGACY_ALTER_TABLE:
//			sb.append("PRAGMA legacy_alter_table=");
//			sb.append(getRandomTextBoolean());
//			break;
		case LEGACY_FORMAT:
			sb.append("PRAGMA legacy_file_format=");
			sb.append(getRandomTextBoolean());
			break;
		case OPTIMIZE:
			sb.append("PRAGMA optimize");
			break;
		case REVERSE_UNORDERED_SELECTS:
			sb.append("PRAGMA reverse_unordered_selects=");
			sb.append(getRandomTextBoolean());
			break;
		case SECURE_DELETE:
			sb.append("PRAGMA main.secure_delete=");
			sb.append(Randomly.fromOptions("true", "false", "FAST"));
			break;
		case SHRINK_MEMORY:
			sb.append("PRAGMA shrink_memory");
			break;
		case SOFT_HEAP_LIMIT:
			sb.append("PRAGMA soft_heap_limit=");
			if (Randomly.getBoolean()) {
				sb.append(0);
			} else {
				sb.append(r.getPositiveInteger());
			}
			break;
		case THREADS:
			sb.append("PRAGMA threads=");
			sb.append(r.getInteger());
			break;
		default:
			throw new AssertionError();
		}
		sb.append(";");
		String pragmaString = sb.toString();
		return new QueryAdapter(pragmaString);
	}

	private static String getRandomTextBoolean() {
		return Randomly.fromOptions("true", "false");
	}

}
