package sqlancer.cockroachdb;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

import sqlancer.Query;
import sqlancer.QueryAdapter;
import sqlancer.Randomly;
import sqlancer.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;

public class CockroachDBSetSessionGenerator {
	
	public static String onOff(CockroachDBGlobalState globalState) {
		return Randomly.fromOptions("true", "false");
	}
	
	// https://www.cockroachlabs.com/docs/stable/set-vars.html
	private enum CockroachDBSetting {
		BYTEA_OUTPUT((g) -> Randomly.fromOptions("hex", "escape", "base64")),
		DEFAULT_INT_SIZE((g) -> Randomly.fromOptions(4, 8)),
//		ENABLE_ZIG_ZAG_JOIN(CockroachDBSetSessionGenerator::onOff),
		DISTSQL((g) -> Randomly.fromOptions("on", "off", "auto", "always")),
//		EXPERIMENTAL_FORCE_SPLIT_AT(CockroachDBSetSessionGenerator::onOff),
		EXPERIMENTAL_SERIAL_NORMALIZATION((g) -> Randomly.fromOptions("'rowid'", "'virtual_sequence'")),
//		EXPERIMENTAL_REORDER_JOINS_LIMIT((g) -> Randomly.getNonCachedInteger()),
		VECTORIZE((g) -> Randomly.fromOptions("auto", "on", "off")),  /* see https://github.com/cockroachdb/cockroach/issues/44133, https://github.com/cockroachdb/cockroach/issues/44207 */
//		EXTRA_FLOAT_DIGITS((g) -> g.getRandomly().getInteger(-15, 3)),
		REORDER_JOINS_LIMIT((g) -> g.getRandomly().getInteger(0, Integer.MAX_VALUE)),
		SQL_SAFE_UPDATES(CockroachDBSetSessionGenerator::onOff),
//		TRACING(CockroachDBSetSessionGenerator::onOff)
		;
		
		private Function<CockroachDBGlobalState, Object> f;
		
		private CockroachDBSetting(Function<CockroachDBGlobalState, Object> f) {
			this.f = f;
		}
	}

	public static Query create(CockroachDBGlobalState globalState) {
		CockroachDBSetting s = Randomly.fromOptions(CockroachDBSetting.values());
		StringBuilder sb = new StringBuilder("SET SESSION ");
		sb.append(s);
		sb.append("=");
		sb.append(s.f.apply(globalState));
		Set<String> errors = new HashSet<>();
		CockroachDBErrors.addTransactionErrors(errors);
		Query q = new QueryAdapter(sb.toString(), errors);
		return q;
	}
	
}
