package sqlancer.hsqldb.ast;

import java.util.ArrayList;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.ast.newast.TableReferenceNode;
import sqlancer.hsqldb.HSQLDBProvider.HSQLDBGlobalState;
import sqlancer.hsqldb.HSQLDBSchema;
import sqlancer.hsqldb.HSQLDBSchema.HSQLDBTable;
import sqlancer.hsqldb.gen.HSQLDBExpressionGenerator;

public class HSQLDBJoin implements Node<HSQLDBExpression> {

    private final TableReferenceNode<HSQLDBExpression, HSQLDBTable> leftTable;
    private final TableReferenceNode<HSQLDBExpression, HSQLDBTable> rightTable;
    private final JoinType joinType;
    private final Node<HSQLDBExpression> onCondition;
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

    public HSQLDBJoin(TableReferenceNode<HSQLDBExpression, HSQLDBTable> leftTable,
            TableReferenceNode<HSQLDBExpression, HSQLDBTable> rightTable, JoinType joinType,
            Node<HSQLDBExpression> whereCondition) {
        this.leftTable = leftTable;
        this.rightTable = rightTable;
        this.joinType = joinType;
        this.onCondition = whereCondition;
    }

    public TableReferenceNode<HSQLDBExpression, HSQLDBTable> getLeftTable() {
        return leftTable;
    }

    public TableReferenceNode<HSQLDBExpression, HSQLDBTable> getRightTable() {
        return rightTable;
    }

    public JoinType getJoinType() {
        return joinType;
    }

    public Node<HSQLDBExpression> getOnCondition() {
        return onCondition;
    }

    private void setOuterType(OuterType outerType) {
        this.outerType = outerType;
    }

    public OuterType getOuterType() {
        return outerType;
    }

    public static List<Node<HSQLDBExpression>> getJoins(
            List<TableReferenceNode<HSQLDBExpression, HSQLDBTable>> tableList, HSQLDBGlobalState globalState) {
        List<Node<HSQLDBExpression>> joinExpressions = new ArrayList<>();
        while (tableList.size() >= 2 && Randomly.getBooleanWithRatherLowProbability()) {
            TableReferenceNode<HSQLDBExpression, HSQLDBTable> leftTable = tableList.remove(0);
            TableReferenceNode<HSQLDBExpression, HSQLDBTable> rightTable = tableList.remove(0);
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

    public static HSQLDBJoin createRightOuterJoin(TableReferenceNode<HSQLDBExpression, HSQLDBTable> left,
            TableReferenceNode<HSQLDBExpression, HSQLDBTable> right, Node<HSQLDBExpression> predicate) {
        return new HSQLDBJoin(left, right, JoinType.RIGHT, predicate);
    }

    public static HSQLDBJoin createLeftOuterJoin(TableReferenceNode<HSQLDBExpression, HSQLDBTable> left,
            TableReferenceNode<HSQLDBExpression, HSQLDBSchema.HSQLDBTable> right, Node<HSQLDBExpression> predicate) {
        return new HSQLDBJoin(left, right, JoinType.LEFT, predicate);
    }

    public static HSQLDBJoin createInnerJoin(TableReferenceNode<HSQLDBExpression, HSQLDBSchema.HSQLDBTable> left,
            TableReferenceNode<HSQLDBExpression, HSQLDBTable> right, Node<HSQLDBExpression> predicate) {
        return new HSQLDBJoin(left, right, JoinType.INNER, predicate);
    }

    public static Node<HSQLDBExpression> createNaturalJoin(TableReferenceNode<HSQLDBExpression, HSQLDBTable> left,
            TableReferenceNode<HSQLDBExpression, HSQLDBTable> right, OuterType naturalJoinType) {
        HSQLDBJoin join = new HSQLDBJoin(left, right, JoinType.NATURAL, null);
        join.setOuterType(naturalJoinType);
        return join;
    }

}
