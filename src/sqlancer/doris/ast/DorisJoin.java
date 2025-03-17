package sqlancer.doris.ast;

import java.util.ArrayList;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.ast.JoinBase;
import sqlancer.common.ast.newast.Join;
import sqlancer.doris.DorisProvider.DorisGlobalState;
import sqlancer.doris.DorisSchema;
import sqlancer.doris.DorisSchema.DorisColumn;
import sqlancer.doris.DorisSchema.DorisTable;
import sqlancer.doris.gen.DorisNewExpressionGenerator;

public class DorisJoin extends JoinBase<DorisExpression>
        implements DorisExpression, Join<DorisExpression, DorisTable, DorisColumn> {

    private final DorisTableReference leftTable;
    private final DorisTableReference rightTable;

    public DorisJoin(DorisTableReference leftTable, DorisTableReference rightTable, JoinType joinType,
            DorisExpression whereCondition) {
        super(joinType, whereCondition);
        this.leftTable = leftTable;
        this.rightTable = rightTable;
    }

    public DorisTableReference getLeftTable() {
        return leftTable;
    }

    public DorisTableReference getRightTable() {
        return rightTable;
    }

    public JoinType getJoinType() {
        return type;
    }

    public DorisExpression getOnCondition() {
        return onClause;
    }

    public static List<DorisJoin> getJoins(List<DorisTableReference> tableList, DorisGlobalState globalState) {
        List<DorisJoin> joinExpressions = new ArrayList<>();
        while (tableList.size() >= 2 && Randomly.getBooleanWithRatherLowProbability()) {
            DorisTableReference leftTable = tableList.remove(0);
            DorisTableReference rightTable = tableList.remove(0);
            List<DorisColumn> columns = new ArrayList<>(leftTable.getTable().getColumns());
            columns.addAll(rightTable.getTable().getColumns());
            DorisNewExpressionGenerator joinGen = new DorisNewExpressionGenerator(globalState).setColumns(columns);
            switch (DorisJoin.JoinType.getRandomForDatabase("DORIS")) {
            case INNER:
                joinExpressions.add(DorisJoin.createInnerJoin(leftTable, rightTable,
                        joinGen.generateExpression(DorisSchema.DorisDataType.BOOLEAN)));
                break;
            case STRAIGHT:
                joinExpressions.add(DorisJoin.createStraightJoin(leftTable, rightTable,
                        joinGen.generateExpression(DorisSchema.DorisDataType.BOOLEAN)));
                break;
            case LEFT:
                joinExpressions.add(DorisJoin.createLeftOuterJoin(leftTable, rightTable,
                        joinGen.generateExpression(DorisSchema.DorisDataType.BOOLEAN)));
                break;
            case RIGHT:
                joinExpressions.add(DorisJoin.createRightOuterJoin(leftTable, rightTable,
                        joinGen.generateExpression(DorisSchema.DorisDataType.BOOLEAN)));
                break;
            default:
                throw new AssertionError();
            }
        }
        return joinExpressions;
    }

    public static DorisJoin createInnerJoin(DorisTableReference left, DorisTableReference right,
            DorisExpression predicate) {
        return new DorisJoin(left, right, JoinType.INNER, predicate);
    }

    public static DorisJoin createStraightJoin(DorisTableReference left, DorisTableReference right,
            DorisExpression predicate) {
        return new DorisJoin(left, right, JoinType.STRAIGHT, predicate);
    }

    public static DorisJoin createRightOuterJoin(DorisTableReference left, DorisTableReference right,
            DorisExpression predicate) {
        return new DorisJoin(left, right, JoinType.RIGHT, predicate);
    }

    public static DorisJoin createLeftOuterJoin(DorisTableReference left, DorisTableReference right,
            DorisExpression predicate) {
        return new DorisJoin(left, right, JoinType.LEFT, predicate);
    }

    @Override
    public void setOnClause(DorisExpression onClause) {
        super.onClause = onClause;
    }
}
