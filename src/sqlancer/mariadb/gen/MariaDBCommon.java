package sqlancer.mariadb.gen;

import sqlancer.Randomly;

public final class MariaDBCommon {

    private MariaDBCommon() {
    }

    public static void addWaitClause(StringBuilder sb) {
        if (Randomly.getBoolean()) {
            sb.append(" WAIT 1");
        } else if (Randomly.getBoolean()) {
            sb.append(" NOWAIT");
        }
    }

}
