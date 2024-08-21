package sqlancer.duckdb.ast;

import java.util.ArrayList;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.ast.newast.Join;
import sqlancer.duckdb.DuckDBProvider.DuckDBGlobalState;
import sqlancer.duckdb.DuckDBSchema.DuckDBColumn;
import sqlancer.duckdb.DuckDBSchema.DuckDBTable;
import sqlancer.duckdb.gen.DuckDBExpressionGenerator;

public class DuckDBJoin implements DuckDBExpression, Join<DuckDBExpression, DuckDBTable, DuckDBColumn> {

    private final DuckDBTableReference leftTable;
    private final DuckDBTableReference rightTable;
    private final JoinType joinType;
    private DuckDBExpression onCondition;
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

    public DuckDBJoin(DuckDBTableReference leftTable, DuckDBTableReference rightTable, JoinType joinType,
            DuckDBExpression whereCondition) {
        this.leftTable = leftTable;
        this.rightTable = rightTable;
        this.joinType = joinType;
        this.onCondition = whereCondition;
    }

    public DuckDBTableReference getLeftTable() {
        return leftTable;
    }

    public DuckDBTableReference getRightTable() {
        return rightTable;
    }

    public JoinType getJoinType() {
        return joinType;
    }

    public DuckDBExpression getOnCondition() {
        return onCondition;
    }

    private void setOuterType(OuterType outerType) {
        this.outerType = outerType;
    }

    public OuterType getOuterType() {
        return outerType;
    }

    public static List<DuckDBExpression> getJoins(List<DuckDBTableReference> tableList, DuckDBGlobalState globalState) {
        List<DuckDBExpression> joinExpressions = new ArrayList<>();
        while (tableList.size() >= 2 && Randomly.getBooleanWithRatherLowProbability()) {
            DuckDBTableReference leftTable = tableList.remove(0);
            DuckDBTableReference rightTable = tableList.remove(0);
            List<DuckDBColumn> columns = new ArrayList<>(leftTable.getTable().getColumns());
            columns.addAll(rightTable.getTable().getColumns());
            DuckDBExpressionGenerator joinGen = new DuckDBExpressionGenerator(globalState).setColumns(columns);
            switch (DuckDBJoin.JoinType.getRandom()) {
            case INNER:
                joinExpressions.add(DuckDBJoin.createInnerJoin(leftTable, rightTable, joinGen.generateExpression()));
                break;
            case NATURAL:
                joinExpressions.add(DuckDBJoin.createNaturalJoin(leftTable, rightTable, OuterType.getRandom()));
                break;
            case LEFT:
                joinExpressions
                        .add(DuckDBJoin.createLeftOuterJoin(leftTable, rightTable, joinGen.generateExpression()));
                break;
            case RIGHT:
                joinExpressions
                        .add(DuckDBJoin.createRightOuterJoin(leftTable, rightTable, joinGen.generateExpression()));
                break;
            default:
                throw new AssertionError();
            }
        }
        return joinExpressions;
    }

    public static DuckDBJoin createRightOuterJoin(DuckDBTableReference left, DuckDBTableReference right,
            DuckDBExpression predicate) {
        return new DuckDBJoin(left, right, JoinType.RIGHT, predicate);
    }

    public static DuckDBJoin createLeftOuterJoin(DuckDBTableReference left, DuckDBTableReference right,
            DuckDBExpression predicate) {
        return new DuckDBJoin(left, right, JoinType.LEFT, predicate);
    }

    public static DuckDBJoin createInnerJoin(DuckDBTableReference left, DuckDBTableReference right,
            DuckDBExpression predicate) {
        return new DuckDBJoin(left, right, JoinType.INNER, predicate);
    }

    public static DuckDBExpression createNaturalJoin(DuckDBTableReference left, DuckDBTableReference right,
            OuterType naturalJoinType) {
        DuckDBJoin join = new DuckDBJoin(left, right, JoinType.NATURAL, null);
        join.setOuterType(naturalJoinType);
        return join;
    }

    @Override
    public void setOnClause(DuckDBExpression onClause) {
        this.onCondition = onClause;
    }
}
