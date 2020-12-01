package sqlancer.tidb.gen;

import java.sql.SQLException;
import java.util.function.Function;

import sqlancer.Randomly;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.tidb.TiDBProvider.TiDBGlobalState;

public final class TiDBSetGenerator {

    private TiDBSetGenerator() {
    }

    private enum Action {

        // SQL_MODE("sql_mode", (r) -> Randomly.fromOptions("TRADITIONAL", "ANSI", "POSTGRESQL", "ORACLE")),
        TIDB_OPT_AGG_PUSH_DOWN("tidb_opt_agg_push_down", (r) -> Randomly.fromOptions(0, 1)), //
        TIDB_BUILD_STATS_CONCURRENCY("tidb_build_stats_concurrency", (r) -> Randomly.getNotCachedInteger(0, 500)), //
        TIDB_CHECKSUM_TABLE_CONCURRENCY("tidb_checksum_table_concurrency", (r) -> Randomly.getNotCachedInteger(0, 500)), //
        TIDB_DISTSQL_SCAN_CONCURRENCY("tidb_distsql_scan_concurrency", (r) -> Randomly.getNotCachedInteger(1, 500)), //
        TIDB_INDEX_LOOKUP_SIZE("tidb_index_lookup_size", (r) -> Randomly.getNotCachedInteger(1, 100000)), //
        TIDB_INDEX_LOOKUP_CONCURRENCY("tidb_index_lookup_concurrency", (r) -> Randomly.getNotCachedInteger(1, 100)), //
        TIDB_INDEX_LOOKUP_JOIN_CONCURRENCY("tidb_index_lookup_join_concurrency",
                (r) -> Randomly.getNotCachedInteger(1, 100)), //
        TIDB_HASH_JOIN_CONCURRENCY("tidb_hash_join_concurrency", (r) -> Randomly.getNotCachedInteger(1, 100)), //
        TIDB_INDEX_SERIAL_SCAN_CONCURRENCY("tidb_index_serial_scan_concurrency",
                (r) -> Randomly.getNotCachedInteger(1, 100)), //
        TIDB_PROJECTION_CONCURRENCY("tidb_projection_concurrency", (r) -> Randomly.getNotCachedInteger(1, 100)), //
        TIDB_HASHAGG_PARTIAL_CONCURRENCY("tidb_hashagg_partial_concurrency",
                (r) -> Randomly.getNotCachedInteger(1, 100)), //
        TIDB_HASHAGG_FINAL_CONCURRENCY("tidb_hashagg_final_concurrency", (r) -> Randomly.getNotCachedInteger(1, 100)), //
        TIDB_INDEX_JOIN_BATCH_SIZE("tidb_index_join_batch_size", (r) -> Randomly.getNotCachedInteger(1, 5000)), //
        TIDB_INDEX_SKIP_UTF8_CHECK("tidb_skip_utf8_check", (r) -> Randomly.fromOptions(0, 1)), //
        TIDB_INIT_CHUNK_SIZE("tidb_init_chunk_size", (r) -> Randomly.getNotCachedInteger(1, 32)), //
        TIDB_MAX_CHUNK_SIZE("tidb_max_chunk_size", (r) -> Randomly.getNotCachedInteger(32, 50000)), //
        TIDB_CONSTRAINT_CHECK_IN_PLACE("tidb_constraint_check_in_place", (r) -> Randomly.fromOptions(0, 1)), //
        TIDB_OPT_INSUBQ_TO_JOIN_AND_AGG("tidb_opt_insubq_to_join_and_agg", (r) -> Randomly.fromOptions(0, 1)), //
        TIDB_OPT_CORRELATION_THRESHOLD("tidb_opt_correlation_threshold",
                (r) -> Randomly.fromOptions(0, 0.0001, 0.1, 0.25, 0.50, 0.75, 0.9, 0.9999999, 1)), //
        TIDB_OPT_CORRELATION_EXP_FACTOR("tidb_opt_correlation_exp_factor",
                (r) -> Randomly.getNotCachedInteger(0, 10000)),

        TIDB_ENABLE_WINDOW_FUNCTION("tidb_enable_window_function", (r) -> Randomly.fromOptions(0, 1)),

        TIDB_ENABLE_FAST_ANALYZE("tidb_enable_fast_analyze", (r) -> Randomly.fromOptions(0, 1)), //
        TIDB_WAIT_SPLIT_REGION_FINISH("tidb_wait_split_region_finish", (r) -> Randomly.fromOptions(0, 1)),
        // TODO: global
        // TIDB_SCATTER_REGION("tidb_scatter_region", (r) -> Randomly.fromOptions(0, 1));
        TIDB_ENABLE_STMT_SUMMARY("tidb_enable_stmt_summary", (r) -> Randomly.fromOptions(0, 1)), //
        TIDB_ENABLE_CHUNK_RPC("tidb_enable_chunk_rpc", (r) -> Randomly.fromOptions(0, 1));

        private String name;
        private Function<Randomly, Object> prod;

        Action(String name, Function<Randomly, Object> prod) {
            this.name = name;
            this.prod = prod;
        }

    }

    public static SQLQueryAdapter getQuery(TiDBGlobalState globalState) throws SQLException {
        StringBuilder sb = new StringBuilder();
        Action option = Randomly.fromOptions(Action.values());
        sb.append("set @@");
        sb.append(option.name);
        sb.append("=");
        sb.append(option.prod.apply(globalState.getRandomly()));
        return new SQLQueryAdapter(sb.toString());
    }

}
