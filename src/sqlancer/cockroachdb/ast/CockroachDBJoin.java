package sqlancer.cockroachdb.ast;

import java.util.Arrays;

import sqlancer.Randomly;

public class CockroachDBJoin implements CockroachDBExpression {

    private final CockroachDBExpression leftTable;
    private final CockroachDBExpression rightTable;
    private JoinType joinType;
    private CockroachDBExpression onCondition;

    public enum JoinType {
        INNER, LEFT, RIGHT, FULL, CROSS, NATURAL;

        public static JoinType getRandom() {
            return Randomly.fromOptions(values());
        }

        public static JoinType getRandomExcept(JoinType... exclude) {
            JoinType[] values = Arrays.stream(values()).filter(m -> !Arrays.asList(exclude).contains(m))
                    .toArray(JoinType[]::new);
            return Randomly.fromOptions(values);
        }
    }

    public CockroachDBJoin(CockroachDBExpression leftTable, CockroachDBExpression rightTable, JoinType joinType,
            CockroachDBExpression whereCondition) {
        this.leftTable = leftTable;
        this.rightTable = rightTable;
        this.joinType = joinType;
        this.onCondition = whereCondition;
    }

    public CockroachDBExpression getLeftTable() {
        return leftTable;
    }

    public CockroachDBExpression getRightTable() {
        return rightTable;
    }

    public void setJoinType(JoinType joinType) {
        this.joinType = joinType;
    }

    public JoinType getJoinType() {
        return joinType;
    }

    public void setOnCondition(CockroachDBExpression onCondition) {
        this.onCondition = onCondition;
    }

    public CockroachDBExpression getOnCondition() {
        return onCondition;
    }

    public static CockroachDBJoin createJoin(CockroachDBExpression left, CockroachDBExpression right, JoinType type,
            CockroachDBExpression onClause) {
        if (type.compareTo(JoinType.CROSS) >= 0) {
            return new CockroachDBJoin(left, right, type, null);
        } else {
            return new CockroachDBJoin(left, right, type, onClause);
        }
    }
}
