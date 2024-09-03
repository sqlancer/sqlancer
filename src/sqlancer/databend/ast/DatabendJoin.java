package sqlancer.databend.ast;

import java.util.ArrayList;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.ast.newast.Join;
import sqlancer.common.ast.newast.TableReferenceNode;
import sqlancer.databend.DatabendProvider.DatabendGlobalState;
import sqlancer.databend.DatabendSchema;
import sqlancer.databend.DatabendSchema.DatabendColumn;
import sqlancer.databend.DatabendSchema.DatabendTable;
import sqlancer.databend.gen.DatabendNewExpressionGenerator;

public class DatabendJoin implements DatabendExpression, Join<DatabendExpression, DatabendTable, DatabendColumn> {

    private final DatabendTableReference leftTable;
    private final DatabendTableReference rightTable;
    private final JoinType joinType;
    private DatabendExpression onCondition;
    private OuterType outerType;

    public enum JoinType {
        INNER, NATURAL, LEFT, RIGHT;

        public static JoinType getRandom() {
            return Randomly.fromOptions(values());
        }
    }

    public enum OuterType {
        LEFT, RIGHT;

        public static OuterType getRandom() {
            return Randomly.fromOptions(values());
        }
    }

    public DatabendJoin(DatabendTableReference leftTable, DatabendTableReference rightTable, JoinType joinType,
            DatabendExpression whereCondition) {
        this.leftTable = leftTable;
        this.rightTable = rightTable;
        this.joinType = joinType;
        this.onCondition = whereCondition;
    }

    public TableReferenceNode<DatabendExpression, DatabendTable> getLeftTable() {
        return leftTable;
    }

    public TableReferenceNode<DatabendExpression, DatabendTable> getRightTable() {
        return rightTable;
    }

    public JoinType getJoinType() {
        return joinType;
    }

    public DatabendExpression getOnCondition() {
        return onCondition;
    }

    private void setOuterType(OuterType outerType) {
        this.outerType = outerType;
    }

    public OuterType getOuterType() {
        return outerType;
    }

    public static List<DatabendJoin> getJoins(List<DatabendTableReference> tableList, DatabendGlobalState globalState) {
        List<DatabendJoin> joinExpressions = new ArrayList<>();
        while (tableList.size() >= 2 && Randomly.getBooleanWithRatherLowProbability()) {
            DatabendTableReference leftTable = tableList.remove(0);
            DatabendTableReference rightTable = tableList.remove(0);
            List<DatabendColumn> columns = new ArrayList<>(leftTable.getTable().getColumns());
            columns.addAll(rightTable.getTable().getColumns());
            DatabendNewExpressionGenerator joinGen = new DatabendNewExpressionGenerator(globalState)
                    .setColumns(columns);

            switch (JoinType.getRandom()) {
            case INNER:
                joinExpressions.add(DatabendJoin.createInnerJoin(leftTable, rightTable,
                        joinGen.generateExpression(DatabendSchema.DatabendDataType.BOOLEAN)));
                break;
            case NATURAL:
                joinExpressions.add(DatabendJoin.createNaturalJoin(leftTable, rightTable, OuterType.getRandom()));
                break;
            case LEFT:
                joinExpressions.add(DatabendJoin.createLeftOuterJoin(leftTable, rightTable,
                        joinGen.generateExpression(DatabendSchema.DatabendDataType.BOOLEAN)));
                break;
            case RIGHT:
                joinExpressions.add(DatabendJoin.createRightOuterJoin(leftTable, rightTable,
                        joinGen.generateExpression(DatabendSchema.DatabendDataType.BOOLEAN)));
                break;
            default:
                throw new AssertionError();
            }
        }
        return joinExpressions;
    }

    public static DatabendJoin createRightOuterJoin(DatabendTableReference left, DatabendTableReference right,
            DatabendExpression predicate) {
        return new DatabendJoin(left, right, JoinType.RIGHT, predicate);
    }

    public static DatabendJoin createLeftOuterJoin(DatabendTableReference left, DatabendTableReference right,
            DatabendExpression predicate) {
        return new DatabendJoin(left, right, JoinType.LEFT, predicate);
    }

    public static DatabendJoin createInnerJoin(DatabendTableReference left, DatabendTableReference right,
            DatabendExpression predicate) {
        return new DatabendJoin(left, right, JoinType.INNER, predicate);
    }

    public static DatabendJoin createNaturalJoin(DatabendTableReference left, DatabendTableReference right,
            OuterType naturalJoinType) {
        DatabendJoin join = new DatabendJoin(left, right, JoinType.NATURAL, null);
        join.setOuterType(naturalJoinType);
        return join;
    }

    @Override
    public void setOnClause(DatabendExpression onClause) {
        onCondition = onClause;
    }
}
