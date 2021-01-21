package sqlancer.sqlite3.gen;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.sqlite3.SQLite3GlobalState;
import sqlancer.sqlite3.schema.SQLite3Schema;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Table;

public class SQLite3VirtualFTSTableCommandGenerator {

    private final StringBuilder sb = new StringBuilder();
    private final SQLite3Schema s;
    private final Randomly r;
    private final ExpectedErrors errors = new ExpectedErrors();

    public static SQLQueryAdapter create(SQLite3GlobalState globalState) {
        return new SQLite3VirtualFTSTableCommandGenerator(globalState.getSchema(), globalState.getRandomly())
                .generate();
    }

    public SQLite3VirtualFTSTableCommandGenerator(SQLite3Schema s, Randomly r) {
        this.s = s;
        this.r = r;
    }

    private enum Action {
        AUTOMERGE, CRISISMERGE, INTEGRITYCHECK, MERGE, OPTIMIZE, REBUILD, USER_MERGE, PGSZ, RANK; // TODO: delete all
    }

    private SQLQueryAdapter generate() {
        errors.add("has no column named rank");
        SQLite3Table vTable = s.getRandomTableOrBailout(t -> t.isVirtual() && t.getName().startsWith("vt"));
        Action a = Randomly.fromOptions(Action.values());
        switch (a) {
        case AUTOMERGE:
        case CRISISMERGE:
        case USER_MERGE:
            sb.append("INSERT INTO ");
            sb.append(vTable.getName());
            if (a == Action.AUTOMERGE) {
                if (Randomly.getBoolean()) {
                    // FTS5 syntax
                    sb.append(String.format("(%s, rank)", vTable.getName()));
                    sb.append(String.format(" VALUES('automerge', %d)", r.getInteger(0, 16)));
                } else {
                    // FTS3/FTS4 syntax
                    sb.append(String.format("(%s)", vTable.getName()));
                    sb.append(String.format(" VALUES('automerge=%d')", r.getInteger(0, 16)));
                    errors.add("SQL logic error"); // when using the FTS3 syntax on an FTS5 table
                }
            } else if (a == Action.CRISISMERGE) {
                sb.append(String.format("(%s, rank)", vTable.getName()));
                sb.append(String.format(" VALUES('crisismerge', %d)", r.getLong(0, Integer.MAX_VALUE)));
            } else {
                sb.append(String.format("(%s, rank)", vTable.getName()));
                sb.append(String.format(" VALUES('usermerge', %d)", r.getLong(2, 16)));
            }
            break;
        case INTEGRITYCHECK:
            sb.append(
                    String.format("INSERT INTO %s(%s) VALUES('integrity-check');", vTable.getName(), vTable.getName()));
            break;
        case MERGE:
            if (Randomly.getBoolean()) {
                // FTS5 syntax
                sb.append(String.format("INSERT INTO %s(%s, rank) VALUES('merge', %d);\n", vTable.getName(),
                        vTable.getName(), r.getInteger()));
            } else {
                sb.append(String.format("INSERT INTO %s(%s) VALUES('merge=%d,%d');\n", vTable.getName(),
                        vTable.getName(), r.getInteger(), r.getInteger(2, 16)));
                errors.add("SQL logic error"); // when using the FTS3 syntax on an FTS5 table
            }
            break;
        case OPTIMIZE:
            sb.append(String.format("INSERT INTO %s(%s) VALUES('optimize');", vTable.getName(), vTable.getName()));
            break;
        case REBUILD:
            errors.add("'rebuild' may not be used with a contentless fts5 table");
            sb.append(String.format("INSERT INTO %s(%s) VALUES('rebuild');", vTable.getName(), vTable.getName()));
            break;
        case PGSZ:
            sb.append(String.format("INSERT INTO %s(%s, rank) VALUES('pgsz', '%d');", vTable.getName(),
                    vTable.getName(), r.getLong(32, 65536)));
            break;
        case RANK:
            sb.append(String.format("INSERT INTO %s(%s, rank) VALUES('rank', 'bm25(10.0, 5.0)');", vTable.getName(),
                    vTable.getName()));
            break;
        default:
            throw new AssertionError();
        }
        errors.add("The database file is locked");
        return new SQLQueryAdapter(sb.toString(), errors);
    }

}
