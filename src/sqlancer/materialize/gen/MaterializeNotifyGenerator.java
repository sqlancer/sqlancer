package sqlancer.materialize.gen;

import sqlancer.Randomly;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.materialize.MaterializeGlobalState;

public final class MaterializeNotifyGenerator {

    private MaterializeNotifyGenerator() {
    }

    private static String getChannel() {
        return Randomly.fromOptions("asdf", "test");
    }

    public static SQLQueryAdapter createNotify(MaterializeGlobalState globalState) {
        StringBuilder sb = new StringBuilder();
        sb.append("NOTIFY ");
        sb.append(getChannel());
        if (Randomly.getBoolean()) {
            sb.append(", ");
            sb.append("'");
            sb.append(globalState.getRandomly().getString().replace("'", "''"));
            sb.append("'");
        }
        return new SQLQueryAdapter(sb.toString());
    }

    public static SQLQueryAdapter createListen() {
        StringBuilder sb = new StringBuilder();
        sb.append("LISTEN ");
        sb.append(getChannel());
        return new SQLQueryAdapter(sb.toString());
    }

    public static SQLQueryAdapter createUnlisten() {
        StringBuilder sb = new StringBuilder();
        sb.append("UNLISTEN ");
        if (Randomly.getBoolean()) {
            sb.append(getChannel());
        } else {
            sb.append("*");
        }
        return new SQLQueryAdapter(sb.toString());
    }

}
