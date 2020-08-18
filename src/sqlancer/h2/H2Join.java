package sqlancer.h2;

import java.util.ArrayList;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.ast.newast.TableReferenceNode;
import sqlancer.h2.H2Provider.H2GlobalState;
import sqlancer.h2.H2Schema.H2Column;
import sqlancer.h2.H2Schema.H2Table;

public class H2Join implements Node<H2Expression> {

    private final TableReferenceNode<H2Expression, H2Table> leftTable;
    private final TableReferenceNode<H2Expression, H2Table> rightTable;
    private final JoinType joinType;
    private final Node<H2Expression> onCondition;

    public enum JoinType {
        INNER, CROSS, NATURAL, LEFT, RIGHT;

        public static JoinType getRandom() {
            return Randomly.fromOptions(values());
        }
    }

    public H2Join(TableReferenceNode<H2Expression, H2Table> leftTable,
            TableReferenceNode<H2Expression, H2Table> rightTable, JoinType joinType,
            Node<H2Expression> whereCondition) {
        this.leftTable = leftTable;
        this.rightTable = rightTable;
        this.joinType = joinType;
        this.onCondition = whereCondition;
    }

    public TableReferenceNode<H2Expression, H2Table> getLeftTable() {
        return leftTable;
    }

    public TableReferenceNode<H2Expression, H2Table> getRightTable() {
        return rightTable;
    }

    public JoinType getJoinType() {
        return joinType;
    }

    public Node<H2Expression> getOnCondition() {
        return onCondition;
    }

    public static List<Node<H2Expression>> getJoins(List<TableReferenceNode<H2Expression, H2Table>> tableList,
            H2GlobalState globalState) {
        List<Node<H2Expression>> joinExpressions = new ArrayList<>();
        while (tableList.size() >= 2 && Randomly.getBooleanWithRatherLowProbability()) {
            TableReferenceNode<H2Expression, H2Table> leftTable = tableList.remove(0);
            TableReferenceNode<H2Expression, H2Table> rightTable = tableList.remove(0);
            List<H2Column> columns = new ArrayList<>(leftTable.getTable().getColumns());
            columns.addAll(rightTable.getTable().getColumns());
            H2ExpressionGenerator joinGen = new H2ExpressionGenerator(globalState).setColumns(columns);
            JoinType random = H2Join.JoinType.getRandom();
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

    public static H2Join createRightOuterJoin(TableReferenceNode<H2Expression, H2Table> left,
            TableReferenceNode<H2Expression, H2Table> right, Node<H2Expression> predicate) {
        return new H2Join(left, right, JoinType.RIGHT, predicate);
    }

    public static H2Join createLeftOuterJoin(TableReferenceNode<H2Expression, H2Table> left,
            TableReferenceNode<H2Expression, H2Table> right, Node<H2Expression> predicate) {
        return new H2Join(left, right, JoinType.LEFT, predicate);
    }

    public static H2Join createInnerJoin(TableReferenceNode<H2Expression, H2Table> left,
            TableReferenceNode<H2Expression, H2Table> right, Node<H2Expression> predicate) {
        return new H2Join(left, right, JoinType.INNER, predicate);
    }

    public static Node<H2Expression> createNaturalJoin(TableReferenceNode<H2Expression, H2Table> left,
            TableReferenceNode<H2Expression, H2Table> right) {
        return new H2Join(left, right, JoinType.NATURAL, null);
    }

}
