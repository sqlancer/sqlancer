package sqlancer.sqlite3.gen;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.Query;
import sqlancer.common.query.QueryAdapter;
import sqlancer.sqlite3.SQLite3Provider.SQLite3GlobalState;

public final class SQLite3TransactionGenerator {

    private SQLite3TransactionGenerator() {
    }

    public static Query generateCommit(SQLite3GlobalState globalState) {
        StringBuilder sb = new StringBuilder();
        sb.append(Randomly.fromOptions("COMMIT", "END"));
        if (Randomly.getBoolean()) {
            sb.append(" TRANSACTION");
        }
        return new QueryAdapter(sb.toString(), ExpectedErrors.from("no transaction is active",
                "The database file is locked", "FOREIGN KEY constraint failed"), true);
    }

    public static Query generateBeginTransaction(SQLite3GlobalState globalState) {
        StringBuilder sb = new StringBuilder();
        sb.append("BEGIN ");
        if (Randomly.getBoolean()) {
            sb.append(Randomly.fromOptions("DEFERRED", "IMMEDIATE", "EXCLUSIVE"));
        }
        sb.append(" TRANSACTION;");
        return new QueryAdapter(sb.toString(),
                ExpectedErrors.from("cannot start a transaction within a transaction", "The database file is locked"));
    }

    public static Query generateRollbackTransaction(SQLite3GlobalState globalState) {
        // TODO: could be extended by savepoint
        return new QueryAdapter("ROLLBACK TRANSACTION;",
                ExpectedErrors.from("no transaction is active", "The database file is locked"), true);
    }

}
