package sqlancer.postgres.gen;

import java.util.ArrayList;
import java.util.Arrays;

import sqlancer.Randomly;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.postgres.PostgresGlobalState;

public final class PostgresResetGenerator {
    private PostgresResetGenerator() {
    }

    public enum DynamicConfigOption {
        SYNCHRONOUS_COMMIT("synchronous_commit"), WAL_COMPRESSION("wal_compression"), COMMIT_DELAY("commit_delay"),
        COMMIT_SIBLINGS("commit_siblings"),

        TRACK_ACTIVITIES("track_activities"), TRACK_COUNTS("track_counts"), TRACK_IO_TIMING("track_io_timing"),
        TRACK_FUNCTIONS("track_functions"),

        VACUUM_FREEZE_TABLE_AGE("vacuum_freeze_table_age"), VACUUM_FREEZE_MIN_AGE("vacuum_freeze_min_age"),
        VACUUM_MULTIXACT_FREEZE_TABLE_AGE("vacuum_multixact_freeze_table_age"),
        VACUUM_MULTIXACT_FREEZE_MIN_AGE("vacuum_multixact_freeze_min_age"),

        ENABLE_BITMAPSCAN("enable_bitmapscan"), ENABLE_GATHERMERGE("enable_gathermerge"),
        ENABLE_HASHJOIN("enable_hashjoin"), ENABLE_INDEXSCAN("enable_indexscan"),
        ENABLE_INDEXONLYSCAN("enable_indexonlyscan"), ENABLE_MATERIAL("enable_material"),
        ENABLE_MERGEJOIN("enable_mergejoin"), ENABLE_NESTLOOP("enable_nestloop"),
        ENABLE_PARALLEL_APPEND("enable_parallel_append"), ENABLE_PARALLEL_HASH("enable_parallel_hash"),
        ENABLE_PARTITION_PRUNING("enable_partition_pruning"), ENABLE_PARTITIONWISE_JOIN("enable_partitionwise_join"),
        ENABLE_PARTITIONWISE_AGGREGATE("enable_partitionwise_aggregate"), ENABLE_SEQSCAN("enable_seqscan"),
        ENABLE_SORT("enable_sort"), ENABLE_TIDSCAN("enable_tidscan"),

        SEQ_PAGE_COST("seq_page_cost"), RANDOM_PAGE_COST("random_page_cost"), CPU_TUPLE_COST("cpu_tuple_cost"),
        CPU_INDEX_TUPLE_COST("cpu_index_tuple_cost"), CPU_OPERATOR_COST("cpu_operator_cost"),
        PARALLEL_SETUP_COST("parallel_setup_cost"), PARALLEL_TUPLE_COST("parallel_tuple_cost"),
        MIN_PARALLEL_TABLE_SCAN_SIZE("min_parallel_table_scan_size"),
        MIN_PARALLEL_INDEX_SCAN_SIZE("min_parallel_index_scan_size"), EFFECTIVE_CACHE_SIZE("effective_cache_size"),
        JIT_ABOVE_COST("jit_above_cost"), JIT_INLINE_ABOVE_COST("jit_inline_above_cost"),
        JIT_OPTIMIZE_ABOVE_COST("jit_optimize_above_cost"),

        GEQO("geqo"), GEQO_THRESHOLD("geqo_threshold"), GEQO_EFFORT("geqo_effort"), GEQO_POOL_SIZE("geqo_pool_size"),
        GEQO_GENERATIONS("geqo_generations"), GEQO_SELECTION_BIAS("geqo_selection_bias"), GEQO_SEED("geqo_seed"),

        GIN_FUZZY_SEARCH_LIMIT("gin_fuzzy_search_limit"), SYNCHRONIZED_SEQSCANS("synchronize_seqscans"),

        DEFAULT_STATISTICS_TARGET("default_statistics_target"), CONSTRAINT_EXCLUSION("constraint_exclusion"),
        CURSOR_TUPLE_FRACTION("cursor_tuple_fraction"), FROM_COLLAPSE_LIMIT("from_collapse_limit"), JIT("jit"),
        JOIN_COLLAPSE_LIMIT("join_collapse_limit"), PARALLEL_LEADER_PARTICIPATION("parallel_leader_participation"),
        FORCE_PARALLEL_MODE("force_parallel_mode"), PLAN_CACHE_MODE("plan_cache_mode");

        private final String configName;

        DynamicConfigOption(String configName) {
            this.configName = configName;
        }
    }

    public static SQLQueryAdapter create(PostgresGlobalState globalState) {
        StringBuilder sb = new StringBuilder();
        ArrayList<PostgresResetGenerator.DynamicConfigOption> options = new ArrayList<>(
                Arrays.asList(PostgresResetGenerator.DynamicConfigOption.values()));
        PostgresResetGenerator.DynamicConfigOption option = Randomly.fromList(options);
        sb.append("RESET ");
        if (Randomly.getBooleanWithSmallProbability()) {
            sb.append("ALL");
        } else {
            sb.append(option.configName);
        }

        return new SQLQueryAdapter(sb.toString());
    }
}
