package sqlancer.yugabyte.ysql.gen;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.yugabyte.ysql.YSQLErrors;
import sqlancer.yugabyte.ysql.YSQLGlobalState;

public final class YSQLTransactionGenerator {

    private YSQLTransactionGenerator() {
    }

    public static SQLQueryAdapter executeBegin() {
        ExpectedErrors errors = new ExpectedErrors();
        StringBuilder sb = new StringBuilder("BEGIN");
        if (Randomly.getBoolean()) {
            errors.add("SET TRANSACTION ISOLATION LEVEL must be called before any query");
            sb.append(" ISOLATION LEVEL ");
            sb.append(Randomly.fromOptions("SERIALIZABLE", "REPEATABLE READ", "READ COMMITTED"));
        }
        YSQLErrors.addTransactionErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

    public static SQLQueryAdapter setTransactionMode(YSQLGlobalState globalState) {
        ExpectedErrors errors = new ExpectedErrors();
        StringBuilder sb = new StringBuilder("SET TRANSACTION");
        
        boolean addedProperty = false;
        
        // Add isolation level
        if (Randomly.getBoolean()) {
            sb.append(" ISOLATION LEVEL ");
            sb.append(Randomly.fromOptions("SERIALIZABLE", "REPEATABLE READ", "READ COMMITTED"));
            addedProperty = true;
        }
        
        // Add read/write mode
        if (Randomly.getBoolean()) {
            if (addedProperty) {
                sb.append(",");
            }
            sb.append(" ");
            sb.append(Randomly.fromOptions("READ WRITE", "READ ONLY"));
            addedProperty = true;
        }
        
        // Add deferrable mode (only makes sense for SERIALIZABLE READ ONLY)
        if (Randomly.getBooleanWithRatherLowProbability()) {
            if (addedProperty) {
                sb.append(",");
            }
            sb.append(" ");
            sb.append(Randomly.fromOptions("DEFERRABLE", "NOT DEFERRABLE"));
            addedProperty = true;
        }
        
        // Ensure at least one property is set
        if (!addedProperty) {
            // Always add at least one property to avoid syntax error
            sb.append(" ISOLATION LEVEL ");
            sb.append(Randomly.fromOptions("SERIALIZABLE", "REPEATABLE READ", "READ COMMITTED"));
        }
        
        errors.add("SET TRANSACTION ISOLATION LEVEL must be called before any query");
        errors.add("cannot use serializable mode in a hot standby");
        errors.add("SET TRANSACTION must be called before any query");
        YSQLErrors.addTransactionErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

}
