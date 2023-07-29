package sqlancer.stonedb.ast;

import java.util.ArrayList;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.ast.newast.TableReferenceNode;
import sqlancer.stonedb.StoneDBProvider.StoneDBGlobalState;
import sqlancer.stonedb.StoneDBSchema.StoneDBColumn;
import sqlancer.stonedb.StoneDBSchema.StoneDBTable;
import sqlancer.stonedb.gen.StoneDBExpressionGenerator;

public class StoneDBJoin implements Node<StoneDBExpression> {

    public enum JoinType {
        INNER, NATURAL, LEFT, RIGHT;

        public static JoinType getRandom() {
            return Randomly.fromOptions(values());
        }
    }

    public enum NaturalJoinType {
        LEFT, RIGHT;

        public static NaturalJoinType getRandom() {
            return Randomly.fromOptions(values());
        }
    }

    private final TableReferenceNode<StoneDBExpression, StoneDBTable> leftTable;
    private final TableReferenceNode<StoneDBExpression, StoneDBTable> rightTable;
    private final JoinType joinType;
    private final Node<StoneDBExpression> onCondition;
    private NaturalJoinType naturalJoinType;

    public StoneDBJoin(TableReferenceNode<StoneDBExpression, StoneDBTable> leftTable,
            TableReferenceNode<StoneDBExpression, StoneDBTable> rightTable, JoinType joinType,
            Node<StoneDBExpression> onCondition) {
        this.leftTable = leftTable;
        this.rightTable = rightTable;
        this.joinType = joinType;
        this.onCondition = onCondition;
    }

    public TableReferenceNode<StoneDBExpression, StoneDBTable> getLeftTable() {
        return leftTable;
    }

    public TableReferenceNode<StoneDBExpression, StoneDBTable> getRightTable() {
        return rightTable;
    }

    public JoinType getJoinType() {
        return joinType;
    }

    public Node<StoneDBExpression> getOnCondition() {
        return onCondition;
    }

    public NaturalJoinType getNaturalJoinType() {
        return naturalJoinType;
    }

    public void setNaturalJoinType(NaturalJoinType naturalJoinType) {
        this.naturalJoinType = naturalJoinType;
    }

    public static List<Node<StoneDBExpression>> getJoins(
            List<TableReferenceNode<StoneDBExpression, StoneDBTable>> tableList, StoneDBGlobalState globalState) {
        List<Node<StoneDBExpression>> joinExpressions = new ArrayList<>();
        while (tableList.size() >= 2 && Randomly.getBooleanWithRatherLowProbability()) {
            // get two tables to join
            TableReferenceNode<StoneDBExpression, StoneDBTable> leftTable = tableList.remove(0);
            TableReferenceNode<StoneDBExpression, StoneDBTable> rightTable = tableList.remove(0);
            // store all columns in the above two tables
            List<StoneDBColumn> columns = new ArrayList<>(leftTable.getTable().getColumns());
            columns.addAll(rightTable.getTable().getColumns());
            // create a join generator
            StoneDBExpressionGenerator joinGen = new StoneDBExpressionGenerator(globalState).setColumns(columns);
            switch (StoneDBJoin.JoinType.getRandom()) {
            case INNER:
                joinExpressions.add(StoneDBJoin.createInnerJoin(leftTable, rightTable, joinGen.generateExpression()));
                break;
            case NATURAL:
                joinExpressions.add(StoneDBJoin.createNaturalJoin(leftTable, rightTable, NaturalJoinType.getRandom()));
                break;
            case LEFT:
                joinExpressions
                        .add(StoneDBJoin.createLeftOuterJoin(leftTable, rightTable, joinGen.generateExpression()));
                break;
            case RIGHT:
                joinExpressions
                        .add(StoneDBJoin.createRightOuterJoin(leftTable, rightTable, joinGen.generateExpression()));
                break;
            default:
                throw new AssertionError();
            }
        }
        return joinExpressions;
    }

    public static StoneDBJoin createRightOuterJoin(TableReferenceNode<StoneDBExpression, StoneDBTable> left,
            TableReferenceNode<StoneDBExpression, StoneDBTable> right, Node<StoneDBExpression> onClause) {
        return new StoneDBJoin(left, right, JoinType.RIGHT, onClause);
    }

    public static StoneDBJoin createLeftOuterJoin(TableReferenceNode<StoneDBExpression, StoneDBTable> left,
            TableReferenceNode<StoneDBExpression, StoneDBTable> right, Node<StoneDBExpression> onClause) {
        return new StoneDBJoin(left, right, JoinType.LEFT, onClause);
    }

    public static StoneDBJoin createInnerJoin(TableReferenceNode<StoneDBExpression, StoneDBTable> left,
            TableReferenceNode<StoneDBExpression, StoneDBTable> right, Node<StoneDBExpression> onClause) {
        return new StoneDBJoin(left, right, JoinType.INNER, onClause);
    }

    public static Node<StoneDBExpression> createNaturalJoin(TableReferenceNode<StoneDBExpression, StoneDBTable> left,
            TableReferenceNode<StoneDBExpression, StoneDBTable> right, NaturalJoinType naturalJoinType) {
        StoneDBJoin join = new StoneDBJoin(left, right, JoinType.NATURAL, null);
        join.setNaturalJoinType(naturalJoinType);
        return join;
    }
}
