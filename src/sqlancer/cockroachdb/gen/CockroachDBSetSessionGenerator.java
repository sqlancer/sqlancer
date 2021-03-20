package sqlancer.cockroachdb.gen;

import java.util.function.Function;

import sqlancer.Randomly;
import sqlancer.cockroachdb.CockroachDBErrors;
import sqlancer.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;

public final class CockroachDBSetSessionGenerator {

    private CockroachDBSetSessionGenerator() {
    }

    public static String onOff(CockroachDBGlobalState globalState) {
        return Randomly.fromOptions("true", "false");
    }

    // https://www.cockroachlabs.com/docs/stable/set-vars.html
    private enum CockroachDBSetting {
        BYTEA_OUTPUT((g) -> Randomly.fromOptions("hex", "escape", "base64")),
        DEFAULT_INT_SIZE((g) -> Randomly.fromOptions(4, 8)),
        DISTSQL((g) -> Randomly.fromOptions("on", "off", "auto", "always")),
        ENABLE_IMPLICIT_SELECT_FOR_UPDATE(CockroachDBSetSessionGenerator::onOff),
        ENABLE_INSERT_FAST_PATH(CockroachDBSetSessionGenerator::onOff),
        ENABLE_ZIGZAG_JOIN(CockroachDBSetSessionGenerator::onOff),
        // EXPERIMENTAL_ENABLE_HASH_SHARDED_INDEXES(CockroachDBSetSessionGenerator::onOff),
        SERIAL_NORMALIZATION((g) -> Randomly.fromOptions("'rowid'", "'virtual_sequence'")),
        EXTRA_FLOAT_DIGITS((g) -> g.getRandomly().getInteger(-15, 3)),
        REORDER_JOINS_LIMIT((g) -> g.getRandomly().getInteger(0, Integer.MAX_VALUE)), //
        SQL_SAFE_UPDATES((g) -> "off"),
        // TRACING(CockroachDBSetSessionGenerator::onOff)
        /*
         * CockroachDB enables vectorized (column-oriented) execution by default. Row-oriented execution can be enforced
         * by setting vectorized to "off". Some examples of bugs found in the vectorized execution engine are:
         * https://github.com/cockroachdb/cockroach/issues/44133 https://github.com/cockroachdb/cockroach/issues/44207
         *
         */
        VECTORIZE((g) -> Randomly.fromOptions("on", "off"));

        private Function<CockroachDBGlobalState, Object> f;

        CockroachDBSetting(Function<CockroachDBGlobalState, Object> f) {
            this.f = f;
        }
    }

    public static SQLQueryAdapter create(CockroachDBGlobalState globalState) {
        CockroachDBSetting s = Randomly.fromOptions(CockroachDBSetting.values());
        StringBuilder sb = new StringBuilder("SET SESSION ");
        sb.append(s);
        sb.append("=");
        sb.append(s.f.apply(globalState));
        ExpectedErrors errors = new ExpectedErrors();
        CockroachDBErrors.addTransactionErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors);
    }

}
