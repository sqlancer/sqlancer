package sqlancer.duckdb.ast;

import java.util.ArrayList;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.ast.JoinBase;
import sqlancer.common.ast.newast.Join;
import sqlancer.duckdb.DuckDBProvider.DuckDBGlobalState;
import sqlancer.duckdb.DuckDBSchema.DuckDBColumn;
import sqlancer.duckdb.DuckDBSchema.DuckDBTable;
import sqlancer.duckdb.gen.DuckDBExpressionGenerator;

public class DuckDBJoin extends JoinBase<DuckDBExpression> implements DuckDBExpression, Join<DuckDBExpression, DuckDBTable, DuckDBColumn> {

    private final DuckDBTableReference leftTable;
    private final DuckDBTableReference rightTable;
    private OuterType outerType;

    public enum OuterType {
        FULL, LEFT, RIGHT;

        public static OuterType getRandom() {
            return Randomly.fromOptions(values());
        }
    }

    public DuckDBJoin(DuckDBTableReference leftTable, DuckDBTableReference rightTable, JoinType joinType,
            DuckDBExpression whereCondition) {
        super(joinType, whereCondition);
        this.leftTable = leftTable;
        this.rightTable = rightTable;
    }

    public DuckDBTableReference getLeftTable() {
        return leftTable;
    }

    public DuckDBTableReference getRightTable() {
        return rightTable;
    }

    public JoinType getJoinType() {
        return type;
    }

    public DuckDBExpression getOnCondition() {
        return onClause;
    }

    private void setOuterType(OuterType outerType) {
        this.outerType = outerType;
    }

    public OuterType getOuterType() {
        return outerType;
    }

    public static List<DuckDBJoin> getJoins(List<DuckDBTableReference> tableList, DuckDBGlobalState globalState) {
        List<DuckDBJoin> joinExpressions = new ArrayList<>();
        while (tableList.size() >= 2 && Randomly.getBooleanWithRatherLowProbability()) {
            DuckDBTableReference leftTable = tableList.remove(0);
            DuckDBTableReference rightTable = tableList.remove(0);
            List<DuckDBColumn> columns = new ArrayList<>(leftTable.getTable().getColumns());
            columns.addAll(rightTable.getTable().getColumns());
            DuckDBExpressionGenerator joinGen = new DuckDBExpressionGenerator(globalState).setColumns(columns);
            switch (DuckDBJoin.JoinType.getRandomForDatabase("DUCKDB")) {
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

    public static DuckDBJoin createNaturalJoin(DuckDBTableReference left, DuckDBTableReference right,
            OuterType naturalJoinType) {
        DuckDBJoin join = new DuckDBJoin(left, right, JoinType.NATURAL, null);
        join.setOuterType(naturalJoinType);
        return join;
    }

    @Override
    public void setOnClause(DuckDBExpression onClause) {
        super.onClause = onClause;
    }
}
