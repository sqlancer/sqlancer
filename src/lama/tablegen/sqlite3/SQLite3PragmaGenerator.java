package lama.tablegen.sqlite3;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import lama.Main.StateToReproduce;
import lama.Randomly;

public class SQLite3PragmaGenerator {

	private enum Pragma {
		APPLICATION_ID, AUTO_VACUUM, AUTOMATIC_INDEX, CACHE_SIZE, CACHE_SPILL_ENABLED, CACHE_SPILL_SIZE,
		//CASE_SENSITIVE_LIKE, // see
								// https://www.mail-archive.com/sqlite-users@mailinglists.sqlite.org/msg115030.html
		CHECKPOINT_FULLSYNC, JOURNAL_MODE, JOURNAL_SIZE_LIMIT, LEGACY_ALTER_TABLE, OPTIMIZE, // , LEGACY_FORMAT
		REVERSE_UNORDERED_SELECTS, SECURE_DELETE, SHRINK_MEMORY, SOFT_HEAP_LIMIT, THREADS
	}

	public static void insertPragmas(Connection con, StateToReproduce state, boolean afterIndicesCreated)
			throws SQLException {
		if (Randomly.getBoolean()) {
			return; // we just want to insert pragmas in about half the cases
		}
		List<Pragma> pragmas = Randomly.subset(Pragma.values());
		for (Pragma p : pragmas) {
			StringBuilder sb = new StringBuilder();
			switch (p) {
			case APPLICATION_ID:
				sb.append("PRAGMA main.application_id=");
				sb.append(Randomly.getInteger());
				break;
			case AUTO_VACUUM:
				sb.append("PRAGMA main.auto_vacuum=");
				sb.append(Randomly.fromOptions("NONE", "FULL", "INCREMENTAL"));
				break;
			case AUTOMATIC_INDEX:
				sb.append("PRAGMA automatic_index = ");
				sb.append(getRandomTextBoolean());
				break;
//			case BUSY_TIMEOUT: // not useful?
			case CACHE_SIZE:
				sb.append("PRAGMA main.cache_size=");
				sb.append(Randomly.getInteger());
				break;
			case CACHE_SPILL_ENABLED:
				sb.append("PRAGMA cache_spill=");
				sb.append(getRandomTextBoolean());
				break;
			case CACHE_SPILL_SIZE:
				sb.append("PRAGMA main.cache_spill=");
				sb.append(Randomly.getInteger());
				break;
//			case CASE_SENSITIVE_LIKE:
//				if (afterIndicesCreated) {
//					sb.append("PRAGMA case_sensitive_like=");
//					sb.append(Randomly.fromOptions("true", "false"));
//					break;
//				} else {
//					continue;
//				}
			// case CELL_CHECK_SIZE // not useful?
			case CHECKPOINT_FULLSYNC:
				sb.append("PRAGMA checkpoint_fullfsync=");
				sb.append(getRandomTextBoolean());
				break;
			case JOURNAL_MODE:
				sb.append("PRAGMA main.journal_mode=");
				sb.append(Randomly.fromOptions("DELETE", "TRUNCATE", "PERSIST", "MEMORY", "WAL", "OFF"));
				break;
			case JOURNAL_SIZE_LIMIT:
				sb.append("PRAGMA main.journal_size_limit=");
				sb.append(Randomly.getInteger());
				break;
			case LEGACY_ALTER_TABLE:
				sb.append("PRAGMA legacy_alter_table=");
				sb.append(getRandomTextBoolean());
				break;
//			case LEGACY_FORMAT:
//				sb.append("PRAGMA legacy_file_format=");
//				sb.append(getRandomTextBoolean());
//				break;
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
				sb.append(Randomly.getPositiveInteger());
				break;
			case THREADS:
				sb.append("PRAGMA threads=");
				sb.append(Randomly.getInteger());
				break;
			default:
				throw new AssertionError();
			}
			sb.append(";");
			String pragmaString = sb.toString();
			state.statements.add(pragmaString);
			try (Statement s = con.createStatement()) {
				s.execute(pragmaString);
			}
		}
	}

	private static String getRandomTextBoolean() {
		return Randomly.fromOptions("true", "false");
	}

}
