package sqlancer.sqlite3.gen;

import java.sql.SQLException;
import java.util.function.Supplier;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.sqlite3.SQLite3GlobalState;

public class SQLite3PragmaGenerator {

    /**
     * Not all pragmas are generated.
     *
     * <ul>
     * <li>case_sensitive_like is not generated since the tool discovered that it has some conceptual issues, see
     * https://www.sqlite.org/src/info/a340eef47b0cad5.</li>
     * <li>legacy_alter_table is not generated since it does not work well with the ALTER command (see docs)</li>
     * <li>journal_mode=off is generated, since it can corrupt the database, see
     * https://www.sqlite.org/src/tktview?name=f4ec250930</li>
     * <li>temp_store deletes all existing temporary tables</li>
     * </ul>
     */
    private enum Pragma {
        APPLICATION_ID, //
        AUTO_VACUUM, //
        AUTOMATIC_INDEX, //
        BUSY_TIMEOUT, //
        CACHE_SIZE, //
        CACHE_SPILL_ENABLED, //
        CACHE_SPILL_SIZE, /* CASE_SENSITIVE_LIKE */ CELL_SIZE_CHECK, CHECKPOINT_FULLSYNC, DEFAULT_CACHE_SIZE,
        DEFER_FOREIGN_KEY, /*
                            * ENCODING,
                            */
        FOREIGN_KEYS, IGNORE_CHECK_CONSTRAINTS, INCREMENTAL_VACUUM, INTEGRITY_CHECK, JOURNAL_MODE, JOURNAL_SIZE_LIMIT,
        /*
         * LEGACY_ALTER_TABLE
         */ OPTIMIZE, LEGACY_FORMAT, LOCKING_MODE, MMAP_SIZE, RECURSIVE_TRIGGERS, REVERSE_UNORDERED_SELECTS,
        SECURE_DELETE, SHRINK_MEMORY, SOFT_HEAP_LIMIT, //
        STATS, //
        SHORT_COLUMN_NAMES,
        /* TEMP_STORE, */ //
        THREADS, //
        WAL_AUTOCHECKPOINT, //
        WAL_CHECKPOINT; //
        // WRITEABLE_SCHEMA

    }

    private final StringBuilder sb = new StringBuilder();
    private final ExpectedErrors errors = new ExpectedErrors();

    public void createPragma(String pragmaName, Supplier<Object> supplier) {
        boolean setSchema = Randomly.getBoolean();
        boolean setValue = Randomly.getBoolean();
        sb.append("PRAGMA ");
        if (setSchema) {
            sb.append(Randomly.fromOptions("main.", "temp."));
        }
        sb.append(pragmaName);
        if (setValue) {
            Object value = supplier.get();
            if (value != null) {
                sb.append(" = ");
                sb.append(supplier.get());
            }
        }
    }

    public SQLQueryAdapter insert(SQLite3GlobalState globalState) {
        Randomly r = globalState.getRandomly();
        Pragma p = Randomly.fromOptions(Pragma.values());
        switch (p) {
        case APPLICATION_ID:
            createPragma("application_id", () -> Randomly.getNonCachedInteger());
            break;
        case AUTO_VACUUM:
            createPragma("auto_vacuum", () -> Randomly.fromOptions("NONE", "FULL", "INCREMENTAL"));
            break;
        case AUTOMATIC_INDEX:
            createPragma("automatic_index", () -> getRandomTextBoolean());
            break;
        case BUSY_TIMEOUT:
            createPragma("busy_timeout", () -> {
                if (Randomly.getBoolean()) {
                    return 0;
                } else {
                    return Math.max(10000, Randomly.getNonCachedInteger());
                }

            });
            break;
        case CACHE_SIZE:
            createPragma("cache_size", () -> {
                if (Randomly.getBoolean()) {
                    return 0;
                } else {
                    return Randomly.getNonCachedInteger();
                }
            });
            break;
        case CACHE_SPILL_ENABLED:
            createPragma("cache_spill", () -> getRandomTextBoolean());
            break;
        case CACHE_SPILL_SIZE:
            createPragma("cache_spill", () -> Randomly.getNonCachedInteger());
            break;
        case CELL_SIZE_CHECK:
            createPragma("cell_size_check", () -> getRandomTextBoolean());
            break;
        case CHECKPOINT_FULLSYNC:
            createPragma("checkpoint_fullfsync", () -> getRandomTextBoolean());
            break;
        case DEFAULT_CACHE_SIZE:
            createPragma("default_cache_size", () -> r.getInteger());
            break;
        case DEFER_FOREIGN_KEY:
            createPragma("defer_foreign_keys", () -> getRandomTextBoolean());
            break;
        // TODO: [SQLITE_ERROR] SQL error or missing database (attached databases must
        // use the same text encoding as main database)
        // case ENCODING:
        // sb.append("PRAGMA main.encoding = \"");
        // String encoding = Randomly.fromOptions("UTF-8", "UTF-16", "UTF-16be", "UTF-16le");
        // sb.append(encoding);
        // sb.append("\";\n");
        // sb.append("PRAGMA temp.encoding = \"");
        // sb.append(encoding);
        // sb.append("\"");
        // break;
        case FOREIGN_KEYS:
            createPragma("foreign_keys", () -> getRandomTextBoolean());
            break;
        case IGNORE_CHECK_CONSTRAINTS:
            createPragma("ignore_check_constraints", () -> getRandomTextBoolean());
            break;
        case INCREMENTAL_VACUUM:
            if (Randomly.getBoolean()) {
                createPragma("incremental_vacuum", () -> null);
            } else {
                sb.append(String.format("PRAGMA incremental_vacuum(%d)", r.getInteger()));
            }
            break;
        case INTEGRITY_CHECK:
            createPragma("integrity_check", () -> null);
            break;
        case JOURNAL_MODE:
            // OFF is no longer generated, since it might corrupt the database upon failed
            // index creation, see https://www.sqlite.org/src/tktview?name=f4ec250930.
            createPragma("journal_mode", () -> Randomly.fromOptions("DELETE", "TRUNCATE", "PERSIST", "MEMORY", "WAL"));
            errors.add("from within a transaction");
            break;
        case JOURNAL_SIZE_LIMIT:
            createPragma("journal_size_limit", () -> {
                if (Randomly.getBoolean()) {
                    return 0;
                } else {
                    return Randomly.getNonCachedInteger();
                }

            });
            break;
        case LEGACY_FORMAT:
            createPragma("legacy_file_format", () -> getRandomTextBoolean());
            break;
        case LOCKING_MODE:
            createPragma("locking_mode", () -> Randomly.fromOptions("NORMAL", "EXCLUSIVE"));
            break;
        case MMAP_SIZE:
            createPragma("mmap_size", () -> Randomly.getNonCachedInteger());
            break;
        case OPTIMIZE:
            createPragma("optimize", () -> null);
            break;
        case RECURSIVE_TRIGGERS:
            createPragma("recursive_triggers", () -> getRandomTextBoolean());
            break;
        case REVERSE_UNORDERED_SELECTS:
            createPragma("reverse_unordered_selects", () -> getRandomTextBoolean());
            break;
        case SECURE_DELETE:
            createPragma("secure_delete", () -> Randomly.fromOptions("true", "false", "FAST"));
            break;
        case SHORT_COLUMN_NAMES:
            createPragma("short_column_names", () -> getRandomTextBoolean());
            break;
        case SHRINK_MEMORY:
            createPragma("shrink_memory", () -> null);
            break;
        case SOFT_HEAP_LIMIT:
            createPragma("soft_heap_limit", () -> {
                if (Randomly.getBoolean()) {
                    return 0;
                } else {
                    return r.getPositiveInteger();
                }
            });
            break;
        case STATS:
            createPragma("stats", () -> null);
            break;
        // case TEMP_STORE:
        // createPragma("temp_store", () -> Randomly.fromOptions("DEFAULT", "FILE", "MEMORY"));
        // break;
        case THREADS:
            createPragma("threads", () -> Randomly.getNonCachedInteger());
            break;
        case WAL_AUTOCHECKPOINT:
            createPragma("wal_autocheckpoint", () -> Randomly.getNonCachedInteger());
            break;
        case WAL_CHECKPOINT:
            sb.append("PRAGMA wal_checkpoint(");
            sb.append(Randomly.fromOptions("PASSIVE", "FULL", "RESTART", "TRUNCATE"));
            sb.append(")");
            errors.add("database table is locked");
            break;
        default:
            throw new AssertionError(p);
        }
        sb.append(";");
        String pragmaString = sb.toString();
        errors.add("The database file is locked");
        return new SQLQueryAdapter(pragmaString, errors);
    }

    public static SQLQueryAdapter insertPragma(SQLite3GlobalState globalState) throws SQLException {
        return new SQLite3PragmaGenerator().insert(globalState);
    }

    private static String getRandomTextBoolean() {
        return Randomly.fromOptions("true", "false");
    }

}
