package sqlancer.doris.ast;

import sqlancer.Randomly;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.ast.newast.TableReferenceNode;
import sqlancer.doris.gen.DorisExpressionGenerator;
import sqlancer.doris.DorisProvider.DorisGlobalState;
import sqlancer.doris.DorisSchema.DorisColumn;
import sqlancer.doris.DorisSchema.DorisTable;

import java.util.ArrayList;
import java.util.List;

public class DorisJoin implements Node<DorisExpression> {

    private final TableReferenceNode<DorisExpression, DorisTable> leftTable;
    private final TableReferenceNode<DorisExpression, DorisTable> rightTable;
    private final JoinType joinType;
    private final Node<DorisExpression> onCondition;
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

    private void setOuterType(OuterType outerType) {
        this.outerType = outerType;
    }

    public OuterType getOuterType() {
        return outerType;
    }

    public static List<Node<DorisExpression>> getJoins(
            List<TableReferenceNode<DorisExpression, DorisTable>> tableList, DorisGlobalState globalState) {
        List<Node<DorisExpression>> joinExpressions = new ArrayList<>();
        while (tableList.size() >= 2 && Randomly.getBooleanWithRatherLowProbability()) {
            TableReferenceNode<DorisExpression, DorisTable> leftTable = tableList.remove(0);
            TableReferenceNode<DorisExpression, DorisTable> rightTable = tableList.remove(0);
            List<DorisColumn> columns = new ArrayList<>(leftTable.getTable().getColumns());
            columns.addAll(rightTable.getTable().getColumns());
            DorisExpressionGenerator joinGen = new DorisExpressionGenerator(globalState).setColumns(columns);
            switch (DorisJoin.JoinType.getRandom()) {
            case INNER:
                joinExpressions.add(DorisJoin.createInnerJoin(leftTable, rightTable, joinGen.generateExpression()));
                break;
            case NATURAL:
                joinExpressions.add(DorisJoin.createNaturalJoin(leftTable, rightTable, OuterType.getRandom()));
                break;
            case LEFT:
                joinExpressions
                        .add(DorisJoin.createLeftOuterJoin(leftTable, rightTable, joinGen.generateExpression()));
                break;
            case RIGHT:
                joinExpressions
                        .add(DorisJoin.createRightOuterJoin(leftTable, rightTable, joinGen.generateExpression()));
                break;
            default:
                throw new AssertionError();
            }
        }
        return joinExpressions;
    }

    public static DorisJoin createRightOuterJoin(TableReferenceNode<DorisExpression, DorisTable> left,
                                                 TableReferenceNode<DorisExpression, DorisTable> right, Node<DorisExpression> predicate) {
        return new DorisJoin(left, right, JoinType.RIGHT, predicate);
    }

    public static DorisJoin createLeftOuterJoin(TableReferenceNode<DorisExpression, DorisTable> left,
                                                TableReferenceNode<DorisExpression, DorisTable> right, Node<DorisExpression> predicate) {
        return new DorisJoin(left, right, JoinType.LEFT, predicate);
    }

    public static DorisJoin createInnerJoin(TableReferenceNode<DorisExpression, DorisTable> left,
                                            TableReferenceNode<DorisExpression, DorisTable> right, Node<DorisExpression> predicate) {
        return new DorisJoin(left, right, JoinType.INNER, predicate);
    }

    public static Node<DorisExpression> createNaturalJoin(TableReferenceNode<DorisExpression, DorisTable> left,
                                                          TableReferenceNode<DorisExpression, DorisTable> right, OuterType naturalJoinType) {
        DorisJoin join = new DorisJoin(left, right, JoinType.NATURAL, null);
        join.setOuterType(naturalJoinType);
        return join;
    }

}
