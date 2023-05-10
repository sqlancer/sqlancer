package sqlancer.doris.ast;

import java.util.ArrayList;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.ast.newast.TableReferenceNode;
import sqlancer.doris.DorisProvider.DorisGlobalState;
import sqlancer.doris.DorisSchema;
import sqlancer.doris.DorisSchema.DorisColumn;
import sqlancer.doris.DorisSchema.DorisTable;
import sqlancer.doris.gen.DorisNewExpressionGenerator;
import sqlancer.doris.visitor.DorisExprToNode;

public class DorisJoin implements Node<DorisExpression> {

    private final TableReferenceNode<DorisExpression, DorisTable> leftTable;
    private final TableReferenceNode<DorisExpression, DorisTable> rightTable;
    private final JoinType joinType;
    private final Node<DorisExpression> onCondition;

    public enum JoinType {
        INNER, STRAIGHT, LEFT, RIGHT;

        public static JoinType getRandom() {
            return Randomly.fromOptions(values());
        }
    }

    public DorisJoin(TableReferenceNode<DorisExpression, DorisTable> leftTable,
            TableReferenceNode<DorisExpression, DorisTable> rightTable, JoinType joinType,
            Node<DorisExpression> whereCondition) {
        this.leftTable = leftTable;
        this.rightTable = rightTable;
        this.joinType = joinType;
        this.onCondition = whereCondition;
    }

    public TableReferenceNode<DorisExpression, DorisTable> getLeftTable() {
        return leftTable;
    }

    public TableReferenceNode<DorisExpression, DorisTable> getRightTable() {
        return rightTable;
    }

    public JoinType getJoinType() {
        return joinType;
    }

    public Node<DorisExpression> getOnCondition() {
        return onCondition;
    }

    public static List<Node<DorisExpression>> getJoins(List<TableReferenceNode<DorisExpression, DorisTable>> tableList,
            DorisGlobalState globalState) {
        List<Node<DorisExpression>> joinExpressions = new ArrayList<>();
        while (tableList.size() >= 2 && Randomly.getBooleanWithRatherLowProbability()) {
            TableReferenceNode<DorisExpression, DorisTable> leftTable = tableList.remove(0);
            TableReferenceNode<DorisExpression, DorisTable> rightTable = tableList.remove(0);
            List<DorisColumn> columns = new ArrayList<>(leftTable.getTable().getColumns());
            columns.addAll(rightTable.getTable().getColumns());
            DorisNewExpressionGenerator joinGen = new DorisNewExpressionGenerator(globalState).setColumns(columns);
            switch (DorisJoin.JoinType.getRandom()) {
            case INNER:
                joinExpressions.add(DorisJoin.createInnerJoin(leftTable, rightTable,
                        DorisExprToNode.cast(joinGen.generateExpression(DorisSchema.DorisDataType.BOOLEAN))));
                break;
            case STRAIGHT:
                joinExpressions.add(DorisJoin.createStraightJoin(leftTable, rightTable,
                        DorisExprToNode.cast(joinGen.generateExpression(DorisSchema.DorisDataType.BOOLEAN))));
                break;
            case LEFT:
                joinExpressions.add(DorisJoin.createLeftOuterJoin(leftTable, rightTable,
                        DorisExprToNode.cast(joinGen.generateExpression(DorisSchema.DorisDataType.BOOLEAN))));
                break;
            case RIGHT:
                joinExpressions.add(DorisJoin.createRightOuterJoin(leftTable, rightTable,
                        DorisExprToNode.cast(joinGen.generateExpression(DorisSchema.DorisDataType.BOOLEAN))));
                break;
            default:
                throw new AssertionError();
            }
        }
        return joinExpressions;
    }

    public static DorisJoin createInnerJoin(TableReferenceNode<DorisExpression, DorisTable> left,
            TableReferenceNode<DorisExpression, DorisTable> right, Node<DorisExpression> predicate) {
        return new DorisJoin(left, right, JoinType.INNER, predicate);
    }

    public static DorisJoin createStraightJoin(TableReferenceNode<DorisExpression, DorisTable> left,
            TableReferenceNode<DorisExpression, DorisTable> right, Node<DorisExpression> predicate) {
        return new DorisJoin(left, right, JoinType.STRAIGHT, predicate);
    }

    public static DorisJoin createRightOuterJoin(TableReferenceNode<DorisExpression, DorisTable> left,
            TableReferenceNode<DorisExpression, DorisTable> right, Node<DorisExpression> predicate) {
        return new DorisJoin(left, right, JoinType.RIGHT, predicate);
    }

    public static DorisJoin createLeftOuterJoin(TableReferenceNode<DorisExpression, DorisTable> left,
            TableReferenceNode<DorisExpression, DorisTable> right, Node<DorisExpression> predicate) {
        return new DorisJoin(left, right, JoinType.LEFT, predicate);
    }
}
