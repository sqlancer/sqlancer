package sqlancer.datafusion.ast;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.ast.newast.TableReferenceNode;
import sqlancer.datafusion.DataFusionProvider.DataFusionGlobalState;
import sqlancer.datafusion.DataFusionSchema;
import sqlancer.datafusion.DataFusionSchema.DataFusionColumn;
import sqlancer.datafusion.DataFusionSchema.DataFusionTable;
import sqlancer.datafusion.gen.DataFusionExpressionGenerator;

/*
    NOT IMPLEMENTED YET
 */
public class DataFusionJoin implements Node<DataFusionExpression> {

    private final TableReferenceNode<DataFusionExpression, DataFusionTable> leftTable;
    private final TableReferenceNode<DataFusionExpression, DataFusionTable> rightTable;
    private final JoinType joinType;
    private final Node<DataFusionExpression> onCondition;

    public DataFusionJoin(TableReferenceNode<DataFusionExpression, DataFusionTable> leftTable,
            TableReferenceNode<DataFusionExpression, DataFusionTable> rightTable, JoinType joinType,
            Node<DataFusionExpression> whereCondition) {
        this.leftTable = leftTable;
        this.rightTable = rightTable;
        this.joinType = joinType;
        this.onCondition = whereCondition;
    }

    public static List<Node<DataFusionExpression>> getJoins(List<DataFusionTable> tables,
            DataFusionGlobalState globalState) {
        // [t1_join_t2, t1_join_t3, ...]
        List<TableReferenceNode<DataFusionExpression, DataFusionTable>> tableList = tables.stream()
                .map(t -> new TableReferenceNode<DataFusionExpression, DataFusionTable>(t))
                .collect(Collectors.toList());
        List<Node<DataFusionExpression>> joinExpressions = new ArrayList<>();
        while (tableList.size() >= 2 && Randomly.getBooleanWithRatherLowProbability()) {
            TableReferenceNode<DataFusionExpression, DataFusionTable> leftTable = tableList.remove(0);
            TableReferenceNode<DataFusionExpression, DataFusionTable> rightTable = tableList.remove(0);
            List<DataFusionColumn> columns = new ArrayList<>(leftTable.getTable().getColumns());
            columns.addAll(rightTable.getTable().getColumns());
            // TODO(datafusion) this `joinGen` can generate super chaotic exprsions, maybe we should make it more like a
            // normal join expression
            DataFusionExpressionGenerator joinGen = new DataFusionExpressionGenerator(globalState).setColumns(columns);
            switch (DataFusionJoin.JoinType.getRandom()) {
            case INNER:
                joinExpressions.add(DataFusionJoin.createInnerJoin(leftTable, rightTable,
                        joinGen.generateExpression(DataFusionSchema.DataFusionDataType.BOOLEAN)));
                break;
            default:
                throw new AssertionError();
            }
        }
        return joinExpressions;
    }

    public static DataFusionJoin createInnerJoin(TableReferenceNode<DataFusionExpression, DataFusionTable> left,
            TableReferenceNode<DataFusionExpression, DataFusionTable> right, Node<DataFusionExpression> predicate) {
        return new DataFusionJoin(left, right, JoinType.INNER, predicate);
    }

    public TableReferenceNode<DataFusionExpression, DataFusionTable> getLeftTable() {
        return leftTable;
    }

    public TableReferenceNode<DataFusionExpression, DataFusionTable> getRightTable() {
        return rightTable;
    }

    public JoinType getJoinType() {
        return joinType;
    }

    public Node<DataFusionExpression> getOnCondition() {
        return onCondition;
    }

    public enum JoinType {
        INNER;
        // NATURAL, LEFT, RIGHT;

        public static JoinType getRandom() {
            return Randomly.fromOptions(values());
        }
    }

}
