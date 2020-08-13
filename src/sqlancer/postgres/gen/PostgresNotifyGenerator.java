package sqlancer.postgres.gen;

import sqlancer.Randomly;
import sqlancer.common.query.Query;
import sqlancer.common.query.QueryAdapter;
import sqlancer.postgres.PostgresGlobalState;

public final class PostgresNotifyGenerator {

    private PostgresNotifyGenerator() {
    }

    private static String getChannel() {
        return Randomly.fromOptions("asdf", "test");
    }

    public static Query createNotify(PostgresGlobalState globalState) {
        StringBuilder sb = new StringBuilder();
        sb.append("NOTIFY ");
        sb.append(getChannel());
        if (Randomly.getBoolean()) {
            sb.append(", ");
            sb.append("'");
            sb.append(globalState.getRandomly().getString().replace("'", "''"));
            sb.append("'");
        }
        return new QueryAdapter(sb.toString());
    }

    public static Query createListen() {
        StringBuilder sb = new StringBuilder();
        sb.append("LISTEN ");
        sb.append(getChannel());
        return new QueryAdapter(sb.toString());
    }

    public static Query createUnlisten() {
        StringBuilder sb = new StringBuilder();
        sb.append("UNLISTEN ");
        if (Randomly.getBoolean()) {
            sb.append(getChannel());
        } else {
            sb.append("*");
        }
        return new QueryAdapter(sb.toString());
    }

}
