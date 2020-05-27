package sqlancer.sqlite3.gen;

import java.util.Arrays;

import sqlancer.Query;
import sqlancer.QueryAdapter;
import sqlancer.Randomly;
import sqlancer.sqlite3.SQLite3Provider.SQLite3GlobalState;

public class SQLite3TransactionGenerator {

    public static Query generateCommit(SQLite3GlobalState globalState) {
        StringBuilder sb = new StringBuilder();
        sb.append(Randomly.fromOptions("COMMIT", "END"));
        if (Randomly.getBoolean()) {
            sb.append(" TRANSACTION");
        }
        return new QueryAdapter(sb.toString(),
                Arrays.asList("no transaction is active", "FOREIGN KEY constraint failed"), true);
    }

    public static Query generateBeginTransaction(SQLite3GlobalState globalState) {
        StringBuilder sb = new StringBuilder();
        sb.append("BEGIN ");
        if (Randomly.getBoolean()) {
            sb.append(Randomly.fromOptions("DEFERRED", "IMMEDIATE", "EXCLUSIVE"));
        }
        sb.append(" TRANSACTION;");
        return new QueryAdapter(sb.toString(), Arrays.asList("cannot start a transaction within a transaction"));
    }

    public static Query generateRollbackTransaction(SQLite3GlobalState globalState) {
        // TODO: could be extended by savepoint
        return new QueryAdapter("ROLLBACK TRANSACTION;", Arrays.asList("no transaction is active"), true);
    }

}
