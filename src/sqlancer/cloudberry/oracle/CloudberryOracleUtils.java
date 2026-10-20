package sqlancer.cloudberry.oracle;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import sqlancer.Randomly;
import sqlancer.postgres.PostgresSchema.PostgresColumn;
import sqlancer.postgres.ast.PostgresColumnValue;
import sqlancer.postgres.ast.PostgresExpression;
import sqlancer.postgres.ast.PostgresJoin;
import sqlancer.postgres.ast.PostgresSelect;
import sqlancer.postgres.ast.PostgresSelect.PostgresFromTable;
import sqlancer.postgres.ast.PostgresSelect.PostgresSubquery;

public final class CloudberryOracleUtils {

    private CloudberryOracleUtils() {
    }

    public static List<PostgresExpression> randomFetchColumns(List<PostgresColumn> columns) {
        return Randomly.nonEmptySubset(columns).stream().map(c -> (PostgresExpression) new PostgresColumnValue(c, null))
                .collect(Collectors.toList());
    }

    // GPORCA does not support ONLY and would fall back to the PostgreSQL-based planner.
    public static void removeOnly(PostgresSelect select) {
        select.setFromList(removeOnlyFromTables(select.getFromList()));
        select.setJoinClauses(removeOnlyFromJoins(select.getJoinClauses()));
    }

    public static List<PostgresExpression> removeOnlyFromTables(List<PostgresExpression> tables) {
        return tables.stream().map(CloudberryOracleUtils::removeOnly).collect(Collectors.toList());
    }

    public static List<PostgresJoin> removeOnlyFromJoins(List<PostgresJoin> joins) {
        return joins.stream()
                .map(j -> j.getTableReference() == null ? j
                        : new PostgresJoin(removeOnly(j.getTableReference()), j.getOnClause(), j.getType()))
                .collect(Collectors.toList());
    }

    private static PostgresExpression removeOnly(PostgresExpression table) {
        if (table instanceof PostgresFromTable) {
            return new PostgresFromTable(((PostgresFromTable) table).getTable(), false);
        }
        if (table instanceof PostgresSubquery) {
            removeOnly(((PostgresSubquery) table).getSelect());
        }
        return table;
    }

    // Returns each row of the query as a single text value, so that ComparatorHelper, which only reads the first
    // column, compares whole rows. A whole-row reference such as CAST(r AS TEXT) would be simpler, but GPORCA does
    // not support it and would fall back to the PostgreSQL-based planner.
    public static String wrap(String queryString, int columnCount) {
        String row = IntStream.range(0, columnCount).mapToObj(i -> "COALESCE(CAST(a" + i + " AS TEXT), 'NULL')")
                .collect(Collectors.joining(" || ',' || "));
        String aliases = IntStream.range(0, columnCount).mapToObj(i -> "a" + i).collect(Collectors.joining(", "));
        return "SELECT " + row + " FROM (" + queryString + ") AS r(" + aliases + ")";
    }

}
