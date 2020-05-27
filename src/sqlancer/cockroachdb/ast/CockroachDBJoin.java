package sqlancer.cockroachdb.ast;

import sqlancer.Randomly;

public class CockroachDBJoin implements CockroachDBExpression {

    private final CockroachDBExpression leftTable;
    private final CockroachDBExpression rightTable;
    private final JoinType joinType;
    private final CockroachDBExpression onCondition;
    private OuterType outerType;

    public enum JoinType {
        INNER, NATURAL, CROSS, OUTER;

        public static JoinType getRandom() {
            return Randomly.fromOptions(values());
        }
    }

    public enum OuterType {
        FULL, LEFT, RIGHT;

        public static OuterType getRandom() {
            return Randomly.fromOptions(values());
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

    public JoinType getJoinType() {
        return joinType;
    }

    public CockroachDBExpression getOnCondition() {
        return onCondition;
    }

    public static CockroachDBJoin createNaturalJoin(CockroachDBExpression left, CockroachDBExpression right) {
        return new CockroachDBJoin(left, right, JoinType.NATURAL, null);
    }

    public static CockroachDBJoin createCrossJoin(CockroachDBExpression left, CockroachDBExpression right) {
        return new CockroachDBJoin(left, right, JoinType.CROSS, null);
    }

    public static CockroachDBJoin createOuterJoin(CockroachDBExpression left, CockroachDBExpression right,
            OuterType type, CockroachDBExpression onClause) {
        CockroachDBJoin join = new CockroachDBJoin(left, right, JoinType.OUTER, onClause);
        join.setOuterType(type);
        return join;
    }

    private void setOuterType(OuterType outerType) {
        this.outerType = outerType;
    }

    public OuterType getOuterType() {
        return outerType;
    }

}
