package sqlancer.cockroachdb;

import sqlancer.cockroachdb.ast.CockroachDBJoin;
import sqlancer.common.ast.JoinBase.JoinType;

public class CockroachDBUtils {

    public static boolean selectAndSetNewJoinType(CockroachDBJoin join, JoinType newJoinType) {
        if (join.getJoinType() == JoinType.LEFT || join.getJoinType() == JoinType.RIGHT) { // No invariant relation
            // between LEFT and RIGHT
            // join
            newJoinType = CockroachDBJoin.JoinType.getRandomExcept("COCKROACHDB", JoinType.NATURAL, JoinType.CROSS,
                    JoinType.LEFT, JoinType.RIGHT);
        } else if (join.getJoinType() == JoinType.FULL) {
            newJoinType = CockroachDBJoin.JoinType.getRandomExcept("COCKROACHDB", JoinType.NATURAL, JoinType.CROSS);
        } else if (join.getJoinType() != JoinType.CROSS) {
            newJoinType = CockroachDBJoin.JoinType.getRandomExcept("COCKROACHDB", JoinType.NATURAL, join.getJoinType());
        }
        assert newJoinType != JoinType.NATURAL; // Natural Join is not supported for CERT
        boolean increase = join.getJoinType().ordinal() < newJoinType.ordinal();
        join.setJoinType(newJoinType);
        return increase;
    }

}
