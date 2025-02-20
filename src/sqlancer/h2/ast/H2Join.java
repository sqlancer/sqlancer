package sqlancer.h2.ast;

import java.util.ArrayList;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.ast.JoinBase;
import sqlancer.common.ast.newast.Join;
import sqlancer.h2.H2ExpressionGenerator;
import sqlancer.h2.H2Provider.H2GlobalState;
import sqlancer.h2.H2Schema.H2Column;
import sqlancer.h2.H2Schema.H2Table;

public class H2Join extends JoinBase<H2Expression> implements H2Expression, Join<H2Expression, H2Table, H2Column> {

    private final H2TableReference leftTable;
    private final H2TableReference rightTable;

    public H2Join(H2TableReference leftTable, H2TableReference rightTable, JoinType joinType,
            H2Expression whereCondition) {
        super(joinType, whereCondition);
        this.leftTable = leftTable;
        this.rightTable = rightTable;
    }

    public H2TableReference getLeftTable() {
        return leftTable;
    }

    public H2TableReference getRightTable() {
        return rightTable;
    }

    public JoinType getJoinType() {
        return type;
    }

    public H2Expression getOnCondition() {
        return onClause;
    }

    public static List<H2Join> getJoins(List<H2TableReference> tableList, H2GlobalState globalState) {
        List<H2Join> joinExpressions = new ArrayList<>();
        while (tableList.size() >= 2 && Randomly.getBooleanWithRatherLowProbability()) {
            H2TableReference leftTable = tableList.remove(0);
            H2TableReference rightTable = tableList.remove(0);
            List<H2Column> columns = new ArrayList<>(leftTable.getTable().getColumns());
            columns.addAll(rightTable.getTable().getColumns());
            H2ExpressionGenerator joinGen = new H2ExpressionGenerator(globalState).setColumns(columns);
            JoinType random = H2Join.JoinType.getRandomForDatabase("H2");
            switch (random) {
            case INNER:
                joinExpressions.add(H2Join.createInnerJoin(leftTable, rightTable, joinGen.generateExpression()));
                break;
            case NATURAL:
                joinExpressions.add(H2Join.createNaturalJoin(leftTable, rightTable));
                break;
            case LEFT:
                joinExpressions.add(H2Join.createLeftOuterJoin(leftTable, rightTable, joinGen.generateExpression()));
                break;
            case RIGHT:
                joinExpressions.add(H2Join.createRightOuterJoin(leftTable, rightTable, joinGen.generateExpression()));
                break;
            case CROSS:
                joinExpressions.add(new H2Join(leftTable, rightTable, JoinType.CROSS, null));
                break;
            default:
                throw new AssertionError(random);
            }
        }
        return joinExpressions;
    }

    public static H2Join createRightOuterJoin(H2TableReference left, H2TableReference right, H2Expression predicate) {
        return new H2Join(left, right, JoinType.RIGHT, predicate);
    }

    public static H2Join createLeftOuterJoin(H2TableReference left, H2TableReference right, H2Expression predicate) {
        return new H2Join(left, right, JoinType.LEFT, predicate);
    }

    public static H2Join createInnerJoin(H2TableReference left, H2TableReference right, H2Expression predicate) {
        return new H2Join(left, right, JoinType.INNER, predicate);
    }

    public static H2Join createNaturalJoin(H2TableReference left, H2TableReference right) {
        return new H2Join(left, right, JoinType.NATURAL, null);
    }

    @Override
    public void setOnClause(H2Expression onClause) {
        super.onClause = onClause;
    }

}
