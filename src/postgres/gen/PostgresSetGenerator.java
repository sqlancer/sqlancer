package postgres.gen;

import java.util.function.Function;

import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;

public class PostgresSetGenerator {

	private enum ConfigurationOption {
		// https://www.postgresql.org/docs/11/runtime-config-wal.html
		// This parameter can only be set at server start.
//		WAL_LEVEL("wal_level", (r) -> Randomly.fromOptions("replica", "minimal", "logical")),
//		FSYNC("fsync", (r) -> Randomly.fromOptions(1, 0)),
		// https://www.postgresql.org/docs/11/runtime-config-query.html
		ENABLE_BITMAPSCAN("enable_bitmapscan", (r) -> Randomly.fromOptions(1, 0)),
		ENABLE_GATHERMERGE("enable_gathermerge", (r) -> Randomly.fromOptions(1, 0)),
		ENABLE_HASHAGG("enable_hashagg", (r) -> Randomly.fromOptions(1, 0)),
		ENABLE_HASHJOIN("enable_hashjoin", (r) -> Randomly.fromOptions(1, 0)),
		ENABLE_INDEXSCAN("enable_indexscan", (r) -> Randomly.fromOptions(1, 0)),
		ENABLE_INDEXONLYSCAN("enable_indexonlyscan", (r) -> Randomly.fromOptions(1, 0)),
		ENABLE_MATERIAL("enable_material", (r) -> Randomly.fromOptions(1, 0)),
		ENABLE_MERGEJOIN("enable_mergejoin", (r) -> Randomly.fromOptions(1, 0)),
		ENABLE_NESTLOOP("enable_nestloop", (r) -> Randomly.fromOptions(1, 0)),
		ENABLE_PARALLEL_APPEND("enable_parallel_append", (r) -> Randomly.fromOptions(1, 0)),
		ENABLE_PARALLEL_HASH("enable_parallel_hash", (r) -> Randomly.fromOptions(1, 0)),
		ENABLE_PARTITION_PRUNING("enable_partition_pruning", (r) -> Randomly.fromOptions(1, 0)),
		ENABLE_PARTITIONWISE_JOIN("enable_partitionwise_join", (r) -> Randomly.fromOptions(1, 0)),
		ENABLE_PARTITIONWISE_AGGREGATE("enable_partitionwise_aggregate", (r) -> Randomly.fromOptions(1, 0)),
		ENABLE_SEGSCAN("enable_seqscan", (r) -> Randomly.fromOptions(1, 0)),
		ENABLE_SORT("enable_sort", (r) -> Randomly.fromOptions(1, 0)),
		ENABLE_TIDSCAN("enable_tidscan", (r) -> Randomly.fromOptions(1, 0)),
		// 19.7.2. Planner Cost Constants
		// https://www.postgresql.org/docs/current/runtime-config-query.html#RUNTIME-CONFIG-QUERY-CONSTANTS
		SEQ_PAGE_COST("seq_page_cost", (r) -> Randomly.fromOptions(0d, 0.00001, 0.05, 0.1, 1, 10, 10000)),
		RANDOM_PAGE_COST("random_page_cost", (r) -> Randomly.fromOptions(0d, 0.00001, 0.05, 0.1, 1, 10, 10000)),
		CPU_TUPLE_COST("cpu_tuple_cost", (r) -> Randomly.fromOptions(0d, 0.00001, 0.05, 0.1, 1, 10, 10000)),
		CPU_INDEX_TUPLE_COST("cpu_index_tuple_cost", (r) -> Randomly.fromOptions(0d, 0.00001, 0.05, 0.1, 1, 10, 10000)),
		CPU_OPERATOR_COST("cpu_operator_cost", (r) -> Randomly.fromOptions(0d, 0.000001, 0.0025, 0.1, 1, 10, 10000)),
		PARALLEL_SETUP_COST("parallel_setup_cost", (r) -> r.getLong(0, Long.MAX_VALUE)),
		PARALLEL_TUPLE_COST("parallel_tuple_cost", (r) -> r.getLong(0, Long.MAX_VALUE)),
		MIN_PARALLEL_TABLE_SCAN_SIZE("min_parallel_table_scan_size", (r) -> r.getInteger(0, 715827882)),
		MIN_PARALLEL_INDEX_SCAN_SIZE("min_parallel_index_scan_size", (r) -> r.getInteger(0, 715827882)),
		EFFECTIVE_CACHE_SIZE("effective_cache_size", (r) -> r.getInteger(1, 2147483647)),
		JIT_ABOVE_COST("jit_above_cost", (r) -> r.getLong(-1, Long.MAX_VALUE)),
		JIT_INLINE_ABOVE_COST("jit_inline_above_cost", (r) -> r.getLong(-1, Long.MAX_VALUE)),
		JIT_OPTIMIZE_ABOVE_COST("jit_optimize_above_cost", (r) -> r.getLong(-1, Long.MAX_VALUE)),
		// 19.7.3. Genetic Query Optimizer
		// https://www.postgresql.org/docs/current/runtime-config-query.html#RUNTIME-CONFIG-QUERY-GEQO
		GEQO("geqo", (r) -> Randomly.fromOptions(1, 0)),
		GEQO_THRESHOLD("geqo_threshold", (r) -> r.getInteger(2, 2147483647)),
		GEQO_EFFORT("geqo_effort", (r) -> r.getInteger(1, 10)),
		GEQO_POO_SIZE("geqo_pool_size", (r) -> r.getInteger(0, 2147483647)),
		GEQO_GENERATIONS("geqo_generations", (r) -> r.getInteger(0, 2147483647)),
		GEQO_SELECTION_BIAS("geqo_selection_bias", (r) -> Randomly.fromOptions(1.5, 1.8, 2.0)),
		GEQO_SEED("geqo_seed", (r) -> Randomly.fromOptions(0, 0.5, 1)),
		// 19.7.4. Other Planner Options
		// https://www.postgresql.org/docs/current/runtime-config-query.html#RUNTIME-CONFIG-QUERY-OTHER
		DEFAULT_STATISTICS_TARGET("default_statistics_target", (r) -> r.getInteger(1, 10000)),
		CONSTRAINT_EXCLUSION("constraint_exclusion", (r) -> Randomly.fromOptions("on", "off", "partition")),
		CURSOR_TUPLE_FRACTION("cursor_tuple_fraction",
				(r) -> Randomly.fromOptions(0.0, 0.1, 0.000001, 1, 0.5, 0.9999999)),
		FROM_COLLAPSE_LIMIT("from_collapse_limit", (r) -> r.getInteger(1, Integer.MAX_VALUE)),
		JIT("jit", (r) -> Randomly.fromOptions(1, 0)),
		JOIN_COLLAPSE_LIMIT("join_collapse_limit", (r) -> r.getInteger(1, Integer.MAX_VALUE)),
		PARALLEL_LEADER_PARTICIPATION("parallel_leader_participation", (r) -> Randomly.fromOptions(1, 0)),
		FORCE_PARALLEL_MODE("force_parallel_mode", (r) -> Randomly.fromOptions("off", "on", "regress"));

		private String optionName;
		private Function<Randomly, Object> op;

		private ConfigurationOption(String optionName, Function<Randomly, Object> op) {
			this.optionName = optionName;
			this.op = op;
		}
	}

	public static Query create(Randomly r) {
		StringBuilder sb = new StringBuilder();
		ConfigurationOption option = Randomly.fromOptions(ConfigurationOption.values());
		sb.append("SET ");
		if (Randomly.getBoolean()) {
			sb.append(Randomly.fromOptions("SESSION", "LOCAL"));
			sb.append(" ");
		}
		sb.append(option.optionName);
		sb.append("=");
		if (Randomly.getBoolean()) {
			sb.append("DEFAULT");
		} else {
			sb.append(option.op.apply(r));
		}
		return new QueryAdapter(sb.toString());
	}

}
