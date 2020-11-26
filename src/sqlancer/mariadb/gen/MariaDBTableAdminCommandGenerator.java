package sqlancer.mariadb.gen;

import java.sql.SQLException;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLQueryResultCheckAdapter;
import sqlancer.mariadb.MariaDBSchema;
import sqlancer.mariadb.MariaDBSchema.MariaDBTable;

public final class MariaDBTableAdminCommandGenerator {

    private MariaDBTableAdminCommandGenerator() {
    }

    public static SQLQueryAdapter checksumTable(MariaDBSchema newSchema) {
        StringBuilder sb = addCommandAndTables(newSchema, "CHECKSUM TABLE");
        if (Randomly.getBoolean()) {
            sb.append(" ");
            sb.append(Randomly.fromOptions("QUICK", "EXTENDED"));
        }
        return new SQLQueryAdapter(sb.toString());
    }

    public static SQLQueryAdapter repairTable(MariaDBSchema newSchema) {
        StringBuilder sb = addCommandAndTables(newSchema, "REPAIR TABLE");
        if (Randomly.getBoolean()) {
            List<String> subset = Randomly.nonEmptySubset("QUICK", "EXTENDED"); // , "USE_FRM"
            sb.append(" ");
            sb.append(subset.stream().collect(Collectors.joining(" ")));
        }
        return checkForMsgText(sb,
                s -> s.equals("OK") || s.equals("The storage engine for the table doesn't support repair"));
    }

    public static SQLQueryAdapter analyzeTable(MariaDBSchema newSchema) {
        StringBuilder sb = addCommandAndTables(newSchema, "ANALYZE TABLE");
        return checkForMsgText(sb, s -> s.equals("OK") || s.equals("Table is already up to date"));
    }

    public static SQLQueryAdapter checkTable(MariaDBSchema newSchema) {
        StringBuilder sb = addCommandAndTables(newSchema, "CHECK TABLE");
        if (Randomly.getBoolean()) {
            List<String> subset = Randomly.nonEmptySubset("FOR UPGRADE", "QUICK", "FAST", "MEDIUM", "EXTENDED",
                    "CHANGED");
            sb.append(" ");
            sb.append(subset.stream().collect(Collectors.joining(" ")));
        }
        return checkForMsgText(sb, s -> s.equals("OK") || s.equals("Table is already up to date"));
    }

    public static SQLQueryAdapter optimizeTable(MariaDBSchema newSchema) {
        StringBuilder sb = addCommandAndTables(newSchema, "OPTIMIZE TABLE");
        MariaDBCommon.addWaitClause(sb);
        return checkForMsgText(sb,
                s -> s.equals("OK") || s.equals("Table does not support optimize, doing recreate + analyze instead")
                        || s.contentEquals("Table is already up to date"));
    }

    private static SQLQueryAdapter checkForMsgText(StringBuilder sb, Function<String, Boolean> checker) {
        return new SQLQueryResultCheckAdapter(sb.toString(), rs -> {
            try {
                while (rs.next()) {
                    String s = rs.getString("Msg_text");
                    if (!checker.apply(s)) {
                        throw new AssertionError(s);
                    }
                }
            } catch (SQLException e) {
                throw new AssertionError(e);
            }
        });
    }

    private static StringBuilder addCommandAndTables(MariaDBSchema newSchema, String command) {
        StringBuilder sb = new StringBuilder(command);
        sb.append(" ");
        List<MariaDBTable> tableSubset = newSchema.getDatabaseTablesRandomSubsetNotEmpty();
        sb.append(tableSubset.stream().map(t -> t.getName()).collect(Collectors.joining(", ")));
        return sb;
    }

}
