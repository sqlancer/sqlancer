package sqlancer.cnosdb.ast;

import java.util.ArrayList;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.cnosdb.CnosDBGlobalState;
import sqlancer.cnosdb.CnosDBSchema.CnosDBColumn;
import sqlancer.cnosdb.CnosDBSchema.CnosDBDataType;
import sqlancer.cnosdb.gen.CnosDBExpressionGenerator;
public class CnosDBJoin implements CnosDBExpression {

    // refactor join expression
    private final CnosDBExpression rightTable;
    private final CnosDBExpression leftTable;
    private JoinType joinType;
    private CnosDBExpression onCondition;

    public enum JoinType {
        INNER, LEFT, RIGHT, FULL;

        public static JoinType getRandom() {
            return Randomly.fromOptions(values());
        }

    }

    public CnosDBJoin(CnosDBExpression leftTable,CnosDBExpression rightTable, JoinType joinType,
            CnosDBExpression whereCondition) {
        this.leftTable = leftTable;
        this.rightTable = rightTable;
        this.joinType = joinType;
        this.onCondition = whereCondition;
    }

    public CnosDBExpression getLeftTable() {
        return leftTable;
    }

    public CnosDBExpression getRightTable() {
        return rightTable;
    }

    public JoinType getJoinType() {
        return joinType;
    }

    public void setJoinType(JoinType joinType) {
        this.joinType = joinType;
    }

    public CnosDBExpression getOnCondition() {
        return onCondition;
    }

    public static List<CnosDBExpression> getJoins(List<CnosDBExpression> tableList, CnosDBGlobalState globalState) {
        List<CnosDBExpression> joinExpressions = new ArrayList<>();
        while (tableList.size() >= 2 && Randomly.getBooleanWithRatherLowProbability()) {
            CnosDBTableReference leftTable = (CnosDBTableReference) tableList.remove(0);
            CnosDBTableReference rightTable = (CnosDBTableReference) tableList.remove(0);

            List<CnosDBColumn> joinColumns = new ArrayList<>(leftTable.getTable().getColumns());

            joinColumns.addAll(rightTable.getTable().getColumns());
            CnosDBExpressionGenerator gen = new CnosDBExpressionGenerator(globalState).setColumns(joinColumns);

            switch (JoinType.getRandom()) {
                case INNER:
                    joinExpressions.add(createInnerJoin(leftTable, rightTable, gen.generateExpression(CnosDBDataType.BOOLEAN)));
                    break;
                case LEFT:
                    joinExpressions.add(createLeftJoin(leftTable, rightTable, gen.generateExpression(CnosDBDataType.BOOLEAN)));
                    break;
                case RIGHT:
                    joinExpressions.add(createRightJoin(leftTable, rightTable, gen.generateExpression(CnosDBDataType.BOOLEAN)));
                    break;
                case FULL:
                    joinExpressions.add(createFullJoin(leftTable, rightTable, gen.generateExpression(CnosDBDataType.BOOLEAN)));
                    break;
                default:
                    throw new AssertionError();
            }
            
        }
        return joinExpressions;
    }

    public static CnosDBExpression createInnerJoin(CnosDBExpression left, CnosDBExpression right, CnosDBExpression predicate) {
        return new CnosDBJoin(left, right, JoinType.INNER, predicate);
    }

    public static CnosDBExpression createLeftJoin(CnosDBExpression left, CnosDBExpression right, CnosDBExpression predicate) {
        return new CnosDBJoin(left, right, JoinType.LEFT, predicate);
    }

    public static CnosDBExpression createRightJoin(CnosDBExpression left, CnosDBExpression right, CnosDBExpression predicate) {
        return new CnosDBJoin(left, right, JoinType.RIGHT, predicate);
    }

    public static CnosDBExpression createFullJoin(CnosDBExpression left, CnosDBExpression right, CnosDBExpression predicate) {
        return new CnosDBJoin(left, right, JoinType.FULL, predicate);
    }


}
