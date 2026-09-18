package sqlancer.cloudberry.gen;

import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.gen.PostgresDiscardGenerator;

/**
 * Generates DISCARD statements like {@link PostgresDiscardGenerator}, but issues {@code DISCARD TEMP} where it would
 * issue {@code DISCARD ALL}.
 *
 * <p>
 * Cloudberry does not dispatch {@code DISCARD ALL} to the segments (it raises the NOTICE "command without clusterwide
 * effect"), so the temporary tables it drops, and any statistics objects on them, are dropped on the coordinator only.
 * The schema SQLancer reads from the coordinator then no longer matches the segments, and reusing a dropped name fails
 * there, e.g. {@code statistics object "s0" already exists (seg0 ...)}. {@code DISCARD TEMP}, which the NOTICE's hint
 * suggests, is dispatched, so it drops the same temporary objects on every segment too.
 */
public final class CloudberryDiscardGenerator {

    private CloudberryDiscardGenerator() {
    }

    public static SQLQueryAdapter create(PostgresGlobalState globalState) {
        SQLQueryAdapter query = PostgresDiscardGenerator.create(globalState);
        if (!query.getUnterminatedQueryString().equals("DISCARD ALL")) {
            return query;
        }
        return new SQLQueryAdapter("DISCARD TEMP", query.getExpectedErrors(), true);
    }

}
