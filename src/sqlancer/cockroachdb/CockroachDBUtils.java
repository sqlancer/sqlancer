package sqlancer.cockroachdb;

import sqlancer.cockroachdb.ast.CockroachDBJoin;
import sqlancer.common.ast.JoinBase.JoinType;

public final class CockroachDBUtils {


    private CockroachDBUtils() {
    }

    public static boolean selectAndSetNewJoinType(CockroachDBJoin join, JoinType unusedJoinType) {
        JoinType selectedJoinType = unusedJoinType; // Start with the passed value

        if (join.getJoinType() == JoinType.LEFT || join.getJoinType() == JoinType.RIGHT) { // No invariant relation
            // between LEFT and RIGHT join
            selectedJoinType = CockroachDBJoin.JoinType.getRandomExcept("COCKROACHDB", JoinType.NATURAL, JoinType.CROSS,
                    JoinType.LEFT, JoinType.RIGHT);
        } else if (join.getJoinType() == JoinType.FULL) {
            selectedJoinType = CockroachDBJoin.JoinType.getRandomExcept("COCKROACHDB", JoinType.NATURAL, JoinType.CROSS);
        } else if (join.getJoinType() != JoinType.CROSS) {
            selectedJoinType = CockroachDBJoin.JoinType.getRandomExcept("COCKROACHDB", JoinType.NATURAL, join.getJoinType());
        }

        assert selectedJoinType != JoinType.NATURAL; // Natural Join is not supported for CERT
        boolean increase = join.getJoinType().ordinal() < selectedJoinType.ordinal();
        join.setJoinType(selectedJoinType);
        return increase;
    }
}
