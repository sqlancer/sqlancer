package sqlancer.yugabyte.ysql.gen;

import java.util.List;
import java.util.ArrayList;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.yugabyte.ysql.YSQLErrors;
import sqlancer.yugabyte.ysql.YSQLGlobalState;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLTable;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLColumn;

public final class YSQLParallelQueryGenerator {

    private YSQLParallelQueryGenerator() {
    }

    public static SQLQueryAdapter generateParallelQueryTest(YSQLGlobalState globalState) {
        // First check if database is colocated
        boolean isColocated = checkIfColocated(globalState);
        
        if (!isColocated) {
            // Return a no-op query for non-colocated databases
            return new SQLQueryAdapter("SELECT 1", true);
        }
        
        ExpectedErrors errors = new ExpectedErrors();
        StringBuilder sb = new StringBuilder();
        
        // Enable parallel query settings
        List<String> settings = new ArrayList<>();
        settings.add("SET max_parallel_workers_per_gather = " + Randomly.getNotCachedInteger(2, 8));
        settings.add("SET parallel_setup_cost = " + Randomly.getNotCachedInteger(0, 100));
        settings.add("SET parallel_tuple_cost = 0.01");
        settings.add("SET min_parallel_table_scan_size = " + Randomly.getNotCachedInteger(0, 10000));
        settings.add("SET min_parallel_index_scan_size = " + Randomly.getNotCachedInteger(0, 10000));
        settings.add("SET force_parallel_mode = " + Randomly.fromOptions("'off'", "'on'", "'regress'"));
        
        // Add YugabyteDB-specific parallel settings
        settings.add("SET yb_parallel_range_rows = " + Randomly.getNotCachedInteger(1, 10000));
        settings.add("SET enable_parallel_append = " + Randomly.fromOptions("true", "false"));
        settings.add("SET enable_parallel_hash = " + Randomly.fromOptions("true", "false"));
        
        // Execute settings
        try {
            for (String setting : settings) {
                globalState.executeStatement(new SQLQueryAdapter(setting, true));
            }
        } catch (Exception e) {
            // Ignore setting errors
        }
        
        // Generate a parallel query
        YSQLTable table = globalState.getSchema().getRandomTable(t -> !t.isView() && t.getColumns().size() > 2);
        if (table == null) {
            return new SQLQueryAdapter("SELECT 1", true);
        }
        
        QueryType queryType = Randomly.fromOptions(QueryType.values());
        switch (queryType) {
        case AGGREGATE_PARALLEL:
            sb.append(generateParallelAggregate(table, globalState));
            break;
        case JOIN_PARALLEL:
            sb.append(generateParallelJoin(table, globalState));
            break;
        case SCAN_PARALLEL:
            sb.append(generateParallelScan(table, globalState));
            break;
        case PARTITION_PARALLEL:
            sb.append(generateParallelPartitionQuery(table, globalState));
            break;
        default:
            throw new AssertionError(queryType);
        }
        
        YSQLErrors.addCommonFetchErrors(errors);
        YSQLErrors.addCommonExpressionErrors(errors);
        errors.add("could not serialize access");
        errors.add("parallel workers");
        errors.add("parallel worker");
        errors.add("out of shared memory");
        errors.add("too many parallel workers");
        
        return new SQLQueryAdapter(sb.toString(), errors);
    }
    
    private static boolean checkIfColocated(YSQLGlobalState globalState) {
        try {
            SQLQueryAdapter query = new SQLQueryAdapter("SELECT yb_is_database_colocated()", true);
            SQLancerResultSet rs = query.executeAndGet(globalState);
            if (rs != null && rs.next()) {
                String result = rs.getString(1);
                return !"f".equals(result);
            }
        } catch (Exception e) {
            // Ignore and assume not colocated
        }
        return false;
    }
    
    private static String generateParallelAggregate(YSQLTable table, YSQLGlobalState globalState) {
        StringBuilder sb = new StringBuilder("EXPLAIN (ANALYZE, BUFFERS) SELECT ");
        
        // Add aggregate functions
        List<String> aggregates = new ArrayList<>();
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
            YSQLColumn col = table.getRandomColumn();
            String agg = Randomly.fromOptions("COUNT", "SUM", "AVG", "MAX", "MIN");
            aggregates.add(agg + "(" + col.getName() + ")");
        }
        sb.append(String.join(", ", aggregates));
        
        sb.append(" FROM ").append(table.getName());
        
        // Add GROUP BY for parallel aggregation
        if (Randomly.getBoolean()) {
            sb.append(" GROUP BY ");
            List<YSQLColumn> groupCols = table.getRandomNonEmptyColumnSubset(Randomly.smallNumber() + 1);
            sb.append(groupCols.stream()
                .map(YSQLColumn::getName)
                .reduce((a, b) -> a + ", " + b)
                .orElse(""));
        }
        
        return sb.toString();
    }
    
    private static String generateParallelJoin(YSQLTable table1, YSQLGlobalState globalState) {
        YSQLTable table2 = globalState.getSchema().getRandomTable(t -> !t.isView() && !t.equals(table1));
        if (table2 == null) {
            return generateParallelScan(table1, globalState);
        }
        
        StringBuilder sb = new StringBuilder("EXPLAIN (ANALYZE, BUFFERS) SELECT ");
        sb.append("t1.*, t2.* FROM ");
        sb.append(table1.getName()).append(" t1 ");
        sb.append(Randomly.fromOptions("INNER JOIN", "LEFT JOIN", "RIGHT JOIN"));
        sb.append(" ").append(table2.getName()).append(" t2 ON ");
        
        // Generate join condition
        YSQLColumn col1 = table1.getRandomColumn();
        YSQLColumn col2 = table2.getRandomColumn();
        sb.append("t1.").append(col1.getName());
        sb.append(" = ");
        sb.append("t2.").append(col2.getName());
        
        // Add WHERE clause to trigger parallel scan
        if (Randomly.getBoolean()) {
            sb.append(" WHERE ");
            sb.append("t1.").append(table1.getRandomColumn().getName());
            sb.append(" > ").append(Randomly.getNotCachedInteger(0, 1000));
        }
        
        return sb.toString();
    }
    
    private static String generateParallelScan(YSQLTable table, YSQLGlobalState globalState) {
        StringBuilder sb = new StringBuilder("EXPLAIN (ANALYZE, BUFFERS) SELECT ");
        
        // Select columns
        if (Randomly.getBoolean()) {
            sb.append("*");
        } else {
            List<YSQLColumn> cols = table.getRandomNonEmptyColumnSubset();
            sb.append(cols.stream()
                .map(YSQLColumn::getName)
                .reduce((a, b) -> a + ", " + b)
                .orElse("*"));
        }
        
        sb.append(" FROM ").append(table.getName());
        
        // Add WHERE clause with complex condition to encourage parallel scan
        sb.append(" WHERE ");
        for (int i = 0; i < Randomly.smallNumber() + 2; i++) {
            if (i > 0) {
                sb.append(" AND ");
            }
            YSQLColumn col = table.getRandomColumn();
            sb.append(col.getName());
            sb.append(Randomly.fromOptions(" > ", " < ", " = ", " != "));
            sb.append(Randomly.getNotCachedInteger(0, 1000));
        }
        
        return sb.toString();
    }
    
    private static String generateParallelPartitionQuery(YSQLTable table, YSQLGlobalState globalState) {
        StringBuilder sb = new StringBuilder("EXPLAIN (ANALYZE, BUFFERS) ");
        
        // Generate UNION ALL query to test parallel append
        sb.append("SELECT * FROM (");
        
        for (int i = 0; i < Randomly.smallNumber() + 2; i++) {
            if (i > 0) {
                sb.append(" UNION ALL ");
            }
            sb.append("SELECT * FROM ").append(table.getName());
            sb.append(" WHERE ").append(table.getRandomColumn().getName());
            sb.append(" ").append(Randomly.fromOptions(">", "<", "="));
            sb.append(" ").append(Randomly.getNotCachedInteger(0, 1000));
        }
        
        sb.append(") AS combined");
        
        return sb.toString();
    }
    
    private enum QueryType {
        AGGREGATE_PARALLEL,
        JOIN_PARALLEL,
        SCAN_PARALLEL,
        PARTITION_PARALLEL
    }
}