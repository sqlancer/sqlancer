package sqlancer.cockroachdb.gen;

import java.util.function.Function;

import sqlancer.Randomly;
import sqlancer.cockroachdb.CockroachDBErrors;
import sqlancer.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;

public final class CockroachDBSetClusterSettingGenerator {

    private CockroachDBSetClusterSettingGenerator() {
    }

    // https://www.cockroachlabs.com/docs/stable/set-vars.html
    private enum CockroachDBClusterSetting {
        COMPATOR_ENABLED("compactor.enabled", CockroachDBSetSessionGenerator::onOff),
        BUFFER_INCREMENT("kv.bulk_ingest.buffer_increment", (g) -> "'" + Randomly.getUncachedDouble() + "'"),
        BACKPRESSURE_RANGE_SIZE_MULTIPLIER(" kv.range.backpressure_range_size_multiplier",
                (g) -> Randomly.getNotCachedInteger(0, Integer.MAX_VALUE)),
        RANGE_DESCRIPTOR_CACHE_SIZE("kv.range_descriptor_cache.size", (g) -> Randomly.getNonCachedInteger()),
        SQL_DEFAULTS_VECTORIZE_ROW_COUNT_THRESHOLD("sql.defaults.vectorize_row_count_threshold",
                (g) -> Randomly.getNotCachedInteger(0, Integer.MAX_VALUE)),
        // SQL_DEFAULTS_EXPERIMENTAL_OPTIMIZER_FOREIGN_KEYS_ENABLED("sql.defaults.experimental_optimizer_foreign_keys.enabled",
        // CockroachDBSetSessionGenerator::onOff),
        SQL_QUERY_CACHE_ENABLED("sql.query_cache.enabled", CockroachDBSetSessionGenerator::onOff),
        SQL_STATS_HISTOGRAM_COLLECTION_ENABLED("sql.stats.histogram_collection.enabled",
                CockroachDBSetSessionGenerator::onOff);

        private Function<CockroachDBGlobalState, Object> f;
        private String name;

        CockroachDBClusterSetting(String name, Function<CockroachDBGlobalState, Object> f) {
            this.name = name;
            this.f = f;
        }
    }

    public static SQLQueryAdapter create(CockroachDBGlobalState globalState) {
        CockroachDBClusterSetting s = Randomly.fromOptions(CockroachDBClusterSetting.values());
        StringBuilder sb = new StringBuilder("SET CLUSTER SETTING ");
        sb.append(s.name);
        sb.append("=");
        if (Randomly.getBooleanWithRatherLowProbability()) {
            sb.append("DEFAULT");
        } else {
            sb.append(s.f.apply(globalState));
        }
        ExpectedErrors errors = new ExpectedErrors();
        CockroachDBErrors.addTransactionErrors(errors);
        errors.add("setting updated but timed out waiting to read new value");

        CockroachDBErrors.addTransactionErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors);
    }

}
