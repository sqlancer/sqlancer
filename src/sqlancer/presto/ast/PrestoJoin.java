package sqlancer.presto.ast;

import java.util.ArrayList;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.ast.newast.TableReferenceNode;
import sqlancer.presto.PrestoGlobalState;
import sqlancer.presto.PrestoSchema;
import sqlancer.presto.PrestoSchema.PrestoColumn;
import sqlancer.presto.PrestoSchema.PrestoTable;
import sqlancer.presto.gen.PrestoTypedExpressionGenerator;

public class PrestoJoin implements Node<PrestoExpression> {

    private final TableReferenceNode<PrestoExpression, PrestoTable> leftTable;
    private final TableReferenceNode<PrestoExpression, PrestoTable> rightTable;
    private final JoinType joinType;
    private final Node<PrestoExpression> onCondition;
    private OuterType outerType;

    public PrestoJoin(TableReferenceNode<PrestoExpression, PrestoTable> leftTable,
            TableReferenceNode<PrestoExpression, PrestoTable> rightTable, JoinType joinType,
            Node<PrestoExpression> whereCondition) {
        this.leftTable = leftTable;
        this.rightTable = rightTable;
        this.joinType = joinType;
        this.onCondition = whereCondition;
    }

    public static List<Node<PrestoExpression>> getJoins(
            List<TableReferenceNode<PrestoExpression, PrestoTable>> tableList, PrestoGlobalState globalState) {
        List<Node<PrestoExpression>> joinExpressions = new ArrayList<>();
        while (tableList.size() >= 2 && Randomly.getBooleanWithRatherLowProbability()) {
            TableReferenceNode<PrestoExpression, PrestoTable> leftTable = tableList.remove(0);
            TableReferenceNode<PrestoExpression, PrestoTable> rightTable = tableList.remove(0);
            List<PrestoColumn> columns = new ArrayList<>(leftTable.getTable().getColumns());
            columns.addAll(rightTable.getTable().getColumns());
            PrestoTypedExpressionGenerator joinGen = new PrestoTypedExpressionGenerator(globalState)
                    .setColumns(columns);
            switch (JoinType.getRandom()) {
            case INNER:
                joinExpressions.add(PrestoJoin.createInnerJoin(leftTable, rightTable, joinGen.generateExpression(
                        PrestoSchema.PrestoCompositeDataType.fromDataType(PrestoSchema.PrestoDataType.BOOLEAN))));
                break;
            case LEFT:
                joinExpressions.add(PrestoJoin.createLeftOuterJoin(leftTable, rightTable, joinGen.generateExpression(
                        PrestoSchema.PrestoCompositeDataType.fromDataType(PrestoSchema.PrestoDataType.BOOLEAN))));
                break;
            case RIGHT:
                joinExpressions.add(PrestoJoin.createRightOuterJoin(leftTable, rightTable, joinGen.generateExpression(
                        PrestoSchema.PrestoCompositeDataType.fromDataType(PrestoSchema.PrestoDataType.BOOLEAN))));
                break;
            default:
                throw new AssertionError();
            }
        }
        return joinExpressions;
    }

    public static PrestoJoin createRightOuterJoin(TableReferenceNode<PrestoExpression, PrestoTable> left,
            TableReferenceNode<PrestoExpression, PrestoTable> right, Node<PrestoExpression> predicate) {
        return new PrestoJoin(left, right, JoinType.RIGHT, predicate);
    }

    public static PrestoJoin createLeftOuterJoin(TableReferenceNode<PrestoExpression, PrestoTable> left,
            TableReferenceNode<PrestoExpression, PrestoTable> right, Node<PrestoExpression> predicate) {
        return new PrestoJoin(left, right, JoinType.LEFT, predicate);
    }

    public static PrestoJoin createInnerJoin(TableReferenceNode<PrestoExpression, PrestoTable> left,
            TableReferenceNode<PrestoExpression, PrestoTable> right, Node<PrestoExpression> predicate) {
        return new PrestoJoin(left, right, JoinType.INNER, predicate);
    }

    public TableReferenceNode<PrestoExpression, PrestoTable> getLeftTable() {
        return leftTable;
    }

    public TableReferenceNode<PrestoExpression, PrestoTable> getRightTable() {
        return rightTable;
    }

    public JoinType getJoinType() {
        return joinType;
    }

    public Node<PrestoExpression> getOnCondition() {
        return onCondition;
    }

    public OuterType getOuterType() {
        return outerType;
    }

    @SuppressWarnings("unused")
    private void setOuterType(OuterType outerType) {
        this.outerType = outerType;
    }

    public enum JoinType {
        INNER, LEFT, RIGHT;

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

}
