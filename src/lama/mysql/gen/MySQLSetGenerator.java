package lama.mysql.gen;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;

public class MySQLSetGenerator {

	private final Randomly r;
	private final StringBuilder sb = new StringBuilder();

	public MySQLSetGenerator(Randomly r) {
		this.r = r;
	}

	public static Query set(Randomly r) {
		return new MySQLSetGenerator(r).get();
	}

	private enum Action {

//		FLUSH("flush", (r) -> Randomly.fromOptions("OFF", "ON"));

//		CONCURRENT_INSERT("concurrent_insert", (r) -> Randomly.fromOptions("NEVER", "AUTO", "ALWAYS", 0, 1, 2)),
		AUTOCOMMIT("autocommit", (r) -> 1), //
		BIG_TABLES("big_tables", (r) -> Randomly.fromOptions("OFF", "ON")), //
		BULK_INSERT_CACHE_SIZE("bulk_insert_buffer_size", (r) -> r.getLong(0, Long.MAX_VALUE)), // actually:
																								// 18446744073709551615L
		FOREIGN_KEY_CHECKS("foreign_key_checks", (r) -> Randomly.fromOptions(1, 0)),
		HISTOGRAM_GENERATION_MAX_MEM_SIZE("histogram_generation_max_mem_size",
				(r) -> r.getLong(1000000, Long.MAX_VALUE)), // actually: 18446744073709551615
		INTERNAL_TMP_MEM_STORAGE_ENGINE("internal_tmp_mem_storage_engine",
				(r) -> Randomly.fromOptions("TempTable", "MEMORY")),
		JOIN_BUFFER_SIZE("join_buffer_size", (r) -> r.getLong(128, Long.MAX_VALUE)), // actually: 18446744073709547520
//		KEY_BUFFER_SIZE("key_buffer_size", (r) -> r.getLong(8, Long.MAX_VALUE)), // actually: OS_PER_PROCESS_LIMIT
		MAX_HEAP_TABLE_SIZE("max_heap_table_size", (r) -> r.getLong(16384, Long.MAX_VALUE)), // actually:
																								// 1844674407370954752
		MAX_SEEKS_FOR_KEY("max_seeks_for_key", (r) -> r.getLong(1, Long.MAX_VALUE)), // actually: 18446744073709551615
		// global
//		INNODB_FLUSH_METHOD("innodb_flush_method", (r) -> Randomly.fromOptions("fsync", "O_DSYNC", "littlesync", "nosync", "O_DIRECT", "O_DIRECT_NO_FSYNC")),
		// TODO
		OPTIMIZER_PRUNE_LEVEL("optimizer_prune_level", (r) -> Randomly.fromOptions(0, 1)),
		OPTIMIZER_SEARCH_DEPTH("optimizer_search_depth", (r) -> r.getLong(0, 62)),
		OPTIMIZER_SWITCH("optimizer_switch", (r) -> getOptimizerSwitchConfiguration(r)),
		PARSER_MAX_MEM_SIZE("parser_max_mem_size", (r) -> r.getLong(10000000, Long.MAX_VALUE)),
		PRELOAD_BUFFER_SIZE("preload_buffer_size", (r) -> r.getLong(1024, 1073741824)),
		QUERY_PREALLOC_SIZE("query_prealloc_size", (r) -> r.getLong(8192, Long.MAX_VALUE)),
		RANGE_ALLOC_BLOCK_SIZE("range_alloc_block_size", (r) -> r.getLong(4096, Long.MAX_VALUE)),
		RANGE_OPTIMIZER_MAX_MEM_SIZE("range_optimizer_max_mem_size", (r) -> r.getLong(0, Long.MAX_VALUE)),
		READ_BUFFER_SIZE("read_buffer_size", (r) -> r.getLong(8200, 2147479552)),
		READ_RND_BUFFER_SIZE("read_rnd_buffer_size", (r) -> r.getLong(1, 2147483647)),
		// global
//		SCHEMA_DEFINITION_CACHE("schema_definition_cache", (r) -> r.getLong(256, 524288)),
		SORT_BUFFER_SIZE("sort_buffer_size", (r) -> r.getLong(32768, Long.MAX_VALUE)),
		SQL_AUTO_IS_NULL("sql_auto_is_null", (r) -> Randomly.fromOptions("OFF", "ON")),
		SQL_BUFFER_RESULT("sql_buffer_result", (r) -> Randomly.fromOptions("OFF", "ON")),
		SQL_LOG_OFF("sql_log_off", (r) -> Randomly.fromOptions("OFF", "ON")),
		SQL_QUOTE_SHOW_CREATE("sql_quote_show_create", (r) -> Randomly.fromOptions("OFF", "ON")),
		TMP_TABLE_SIZE("tmp_table_size", (r) -> r.getLong(1024, Long.MAX_VALUE)),
		UNIQUE_CHECKS("unique_checks", (r) -> Randomly.fromOptions("OFF", "ON"))
		// TODO: https://dev.mysql.com/doc/refman/8.0/en/switchable-optimizations.html
		;

		private String name;
		private Function<Randomly, Object> prod;

		Action(String name, Function<Randomly, Object> prod) {
			this.name = name;
			this.prod = prod;
		}

		/**
		 * @see https://dev.mysql.com/doc/refman/8.0/en/switchable-optimizations.html
		 */
		private static String getOptimizerSwitchConfiguration(Randomly r) {
			StringBuilder sb = new StringBuilder();
			sb.append("'");
			String[] options = new String[] { /*"batched_key_access", "block_nested_loop","condition_fanout_filter",
					 */"derived_merge", /*"engine_condition_pushdown"};  , "index_condition_pushdown", "use_index_extensions",
					"index_merge", "index_merge_intersection", "index_merge_sort_union", "index_merge_union",
					"use_invisible_indexes", "mrr", "mrr_cost_based", "skip_scan", "semijoin", "duplicateweedout",
					"firstmatch", "loosescan", "materialization", "subquery_materialization_cost_based"  */
			};
			List<String> optionSubset = Randomly.nonEmptySubset(options);
			sb.append(optionSubset.stream().map(s -> s + "=" + Randomly.fromOptions("on", "off"))
					.collect(Collectors.joining(",")));
			sb.append("'");
			return sb.toString();
		}
	}

	private Query get() {
		Action a = Randomly.fromOptions(Action.values());
		sb.append("SET SESSION ");
		sb.append(a.name);
		sb.append(" = ");
		sb.append(a.prod.apply(r));
		return new QueryAdapter(sb.toString());
	}

}
