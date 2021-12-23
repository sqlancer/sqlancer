package sqlancer.h2;

import java.util.function.Function;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.h2.H2Provider.H2GlobalState;

public final class H2SetGenerator {

    private H2SetGenerator() {
    }

    private enum Option {

        ALLOW_LITERALS((r) -> Randomly.fromOptions("ALL")), //
        CACHE_SIZE((r) -> Randomly.getNotCachedInteger(0, 1000000)), //
        BINARY_COLLATION((r) -> Randomly.fromOptions("SIGNED", "UNSIGNED")), //
        UUID_COLLATION((r) -> Randomly.fromOptions("SIGNED", "UNSIGNED")), //
        BUILTIN_ALIAS_OVERRIDE((r) -> Randomly.fromOptions("TRUE", "FALSE")), //
        COLLATION((r) -> Randomly.fromOptions("OFF", "ENGLISH", "ENGLISH STRENGTH PRIMARY",
                "ENGLISH STRENGTH SECONDARY", "ENGLISH STRENGTH TERTIARY", "ENGLISH STRENGTH IDENTICAL")), //
        DEFAULT_NULL_ORDERING((r) -> Randomly.fromOptions("LOW", "HIGH", "FIRST", "LAST")), //
        DEFAULT_TABLE_TYPE((r) -> Randomly.fromOptions("MEMORY", "CACHED")), //
        IGNORECASE((r) -> Randomly.fromOptions("TRUE", "FALSE")), //
        LAZY_QUERY_EXECUTION((r) -> Randomly.fromOptions(0, 1)), //
        MAX_MEMORY_ROWS((r) -> Randomly.getNotCachedInteger(0, 100000)), //
        MAX_MEMORY_UNDO((r) -> Randomly.getNotCachedInteger(0, 100000)), //
        MAX_OPERATION_MEMORY((r) -> Randomly.getNotCachedInteger(0, 100000)), //
        // MODE((r) -> Randomly.fromOptions("REGULAR", "DB2", "DERBY", "HSQLDB", "MSSQLSERVER", "ORACLE",
        // "POSTGRESQL"));
        OPTIMIZE_REUSE_RESULTS((r) -> Randomly.fromOptions(0, 1)), //
        QUERY_STATISTICS((r) -> Randomly.fromOptions("TRUE", "FALSE")), //
        QUERY_STATISTICS_MAX_ENTRIES((r) -> Randomly.getNotCachedInteger(0, 100000)), //
        REFERENTIAL_INTEGRITY((r) -> Randomly.fromOptions("TRUE", "FALSE")); //

        private Function<Randomly, Object> prod;

        Option(Function<Randomly, Object> prod) {
            this.prod = prod;
        }

        public static Option getRandom() {
            return Randomly.fromOptions(Option.values());
        }
    }

    public static SQLQueryAdapter getQuery(H2GlobalState globalState) {
        StringBuilder sb = new StringBuilder();
        Option o = Option.getRandom();
        sb.append("SET ");
        sb.append(o);
        sb.append(" ");
        sb.append(o.prod.apply(globalState.getRandomly()));
        ExpectedErrors errors = new ExpectedErrors();
        errors.add("Collation cannot be changed because there is a data table");
        return new SQLQueryAdapter(sb.toString(), errors);
    }

}
