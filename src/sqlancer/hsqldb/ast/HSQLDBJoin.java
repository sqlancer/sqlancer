package sqlancer.hsqldb.ast;

import sqlancer.Randomly;
import sqlancer.common.ast.newast.Join;
import sqlancer.hsqldb.HSQLDBSchema.HSQLDBColumn;
import sqlancer.hsqldb.HSQLDBSchema.HSQLDBTable;

public class HSQLDBJoin implements HSQLDBExpression, Join<HSQLDBExpression, HSQLDBTable, HSQLDBColumn> {

    private final HSQLDBTableReference leftTable;
    private final HSQLDBTableReference rightTable;
    private final JoinType joinType;
    private HSQLDBExpression onCondition;
    private OuterType outerType;

    public enum JoinType {
        INNER, NATURAL, LEFT, RIGHT;

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

    public HSQLDBJoin(HSQLDBTableReference leftTable, HSQLDBTableReference rightTable, JoinType joinType,
            HSQLDBExpression whereCondition) {
        this.leftTable = leftTable;
        this.rightTable = rightTable;
        this.joinType = joinType;
        this.onCondition = whereCondition;
    }

    public HSQLDBTableReference getLeftTable() {
        return leftTable;
    }

    public HSQLDBTableReference getRightTable() {
        return rightTable;
    }

    public JoinType getJoinType() {
        return joinType;
    }

    public HSQLDBExpression getOnCondition() {
        return onCondition;
    }

    private void setOuterType(OuterType outerType) {
        this.outerType = outerType;
    }

    public OuterType getOuterType() {
        return outerType;
    }

    public static HSQLDBJoin createRightOuterJoin(HSQLDBTableReference left, HSQLDBTableReference right,
            HSQLDBExpression predicate) {
        return new HSQLDBJoin(left, right, JoinType.RIGHT, predicate);
    }

    public static HSQLDBJoin createLeftOuterJoin(HSQLDBTableReference left, HSQLDBTableReference right,
            HSQLDBExpression predicate) {
        return new HSQLDBJoin(left, right, JoinType.LEFT, predicate);
    }

    public static HSQLDBJoin createInnerJoin(HSQLDBTableReference left, HSQLDBTableReference right,
            HSQLDBExpression predicate) {
        return new HSQLDBJoin(left, right, JoinType.INNER, predicate);
    }

    public static HSQLDBJoin createNaturalJoin(HSQLDBTableReference left, HSQLDBTableReference right,
            OuterType naturalJoinType) {
        HSQLDBJoin join = new HSQLDBJoin(left, right, JoinType.NATURAL, null);
        join.setOuterType(naturalJoinType);
        return join;
    }

    @Override
    public void setOnClause(HSQLDBExpression onClause) {
        onCondition = onClause;
    }

}
