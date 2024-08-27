package sqlancer.hsqldb.ast;

import java.util.ArrayList;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.hsqldb.HSQLDBProvider.HSQLDBGlobalState;
import sqlancer.hsqldb.HSQLDBSchema;
import sqlancer.hsqldb.gen.HSQLDBExpressionGenerator;

public class HSQLDBJoin implements HSQLDBExpression {

    private final HSQLDBTableReference leftTable;
    private final HSQLDBTableReference rightTable;
    private final JoinType joinType;
    private final HSQLDBExpression onCondition;
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

    public static List<HSQLDBExpression> getJoins(List<HSQLDBTableReference> tableList, HSQLDBGlobalState globalState) {
        List<HSQLDBExpression> joinExpressions = new ArrayList<>();
        while (tableList.size() >= 2 && Randomly.getBooleanWithRatherLowProbability()) {
            HSQLDBTableReference leftTable = tableList.remove(0);
            HSQLDBTableReference rightTable = tableList.remove(0);
            List<HSQLDBSchema.HSQLDBColumn> columns = new ArrayList<>(leftTable.getTable().getColumns());
            columns.addAll(rightTable.getTable().getColumns());
            HSQLDBExpressionGenerator joinGen = new HSQLDBExpressionGenerator(globalState).setColumns(columns);
            switch (HSQLDBJoin.JoinType.getRandom()) {
            case INNER:
                joinExpressions.add(HSQLDBJoin.createInnerJoin(leftTable, rightTable,
                        joinGen.generateExpression(HSQLDBSchema.HSQLDBCompositeDataType.getRandomWithoutNull())));
                break;
            case NATURAL:
                joinExpressions.add(HSQLDBJoin.createNaturalJoin(leftTable, rightTable, OuterType.getRandom()));
                break;
            case LEFT:
                joinExpressions.add(HSQLDBJoin.createLeftOuterJoin(leftTable, rightTable,
                        joinGen.generateExpression(HSQLDBSchema.HSQLDBCompositeDataType.getRandomWithoutNull())));
                break;
            case RIGHT:
                joinExpressions.add(HSQLDBJoin.createRightOuterJoin(leftTable, rightTable,
                        joinGen.generateExpression(HSQLDBSchema.HSQLDBCompositeDataType.getRandomWithoutNull())));
                break;
            default:
                throw new AssertionError();
            }
        }
        return joinExpressions;
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

    public static HSQLDBExpression createNaturalJoin(HSQLDBTableReference left, HSQLDBTableReference right,
            OuterType naturalJoinType) {
        HSQLDBJoin join = new HSQLDBJoin(left, right, JoinType.NATURAL, null);
        join.setOuterType(naturalJoinType);
        return join;
    }

}
