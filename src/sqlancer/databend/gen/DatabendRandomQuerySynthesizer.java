package sqlancer.databend.gen;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.ast.newast.TableReferenceNode;
import sqlancer.databend.DatabendExprToNode;
import sqlancer.databend.DatabendProvider.DatabendGlobalState;
import sqlancer.databend.DatabendSchema;
import sqlancer.databend.DatabendSchema.DatabendColumn;
import sqlancer.databend.DatabendSchema.DatabendTable;
import sqlancer.databend.DatabendSchema.DatabendTables;
import sqlancer.databend.ast.DatabendColumnValue;
import sqlancer.databend.ast.DatabendConstant;
import sqlancer.databend.ast.DatabendExpression;
import sqlancer.databend.ast.DatabendJoin;
import sqlancer.databend.ast.DatabendSelect;

public final class DatabendRandomQuerySynthesizer {

    private DatabendRandomQuerySynthesizer() {
    }

    public static DatabendSelect generateSelect(DatabendGlobalState globalState, int nrColumns) {
        DatabendTables targetTables = globalState.getSchema().getRandomTableNonEmptyAndViewTables();
        List<DatabendColumn> targetColumns = targetTables.getColumns();
        DatabendNewExpressionGenerator gen = new DatabendNewExpressionGenerator(globalState).setColumns(targetColumns);
        // boolean allowAggregates = Randomly.getBooleanWithSmallProbability();
        List<Node<DatabendExpression>> columns = new ArrayList<>();
        HashSet<DatabendColumnValue> columnOfLeafNode = new HashSet<>();
        gen.setColumnOfLeafNode(columnOfLeafNode);
        int freeColumns = targetColumns.size();
        for (int i = 0; i < nrColumns; i++) {
            // if (allowAggregates && Randomly.getBoolean()) {
            Node<DatabendExpression> column = null;
            if (freeColumns > 0 && Randomly.getBoolean()) {
                column = new DatabendColumnValue(targetColumns.get(freeColumns - 1), null);
                freeColumns -= 1;
                columnOfLeafNode.add((DatabendColumnValue) column);
            } else {
                column = DatabendExprToNode.cast(gen.generateExpression(DatabendSchema.DatabendDataType.BOOLEAN));
            }
            columns.add(column);
        }
        DatabendSelect select = new DatabendSelect();
        boolean isDistinct = Randomly.getBoolean();
        select.setDistinct(isDistinct);
        select.setFetchColumns(columns);
        List<DatabendTable> tables = targetTables.getTables();
        List<TableReferenceNode<DatabendExpression, DatabendTable>> tableList = tables.stream()
                .map(t -> new TableReferenceNode<DatabendExpression, DatabendTable>(t)).collect(Collectors.toList());
        List<Node<DatabendExpression>> joins = DatabendJoin.getJoins(tableList, globalState);
        select.setJoinList(joins);
        select.setFromList(tableList.stream().collect(Collectors.toList()));
        if (Randomly.getBoolean()) {
            select.setWhereClause(
                    DatabendExprToNode.cast(gen.generateExpression(DatabendSchema.DatabendDataType.BOOLEAN)));
        }

        List<Node<DatabendExpression>> noExprColumns = new ArrayList<>(columnOfLeafNode);

        if (Randomly.getBoolean() && !noExprColumns.isEmpty() && !isDistinct) {
            select.setOrderByExpressions(Randomly.nonEmptySubset(noExprColumns));
            // TODO (for SELECT DISTINCT, ORDER BY expressions must appear in select list) isDistinct
            // 需要orderby输入每个select list，可以用数字代替比如：1,2,3...
        }

        if (Randomly.getBoolean()) { // 可能产生新的column叶子结点
            select.setHavingClause(DatabendExprToNode.cast(gen.generateHavingClause()));
        }

        noExprColumns = new ArrayList<>(columnOfLeafNode);

        if (Randomly.getBoolean() && !noExprColumns.isEmpty()) {
            select.setGroupByExpressions(noExprColumns);
        }

        if (Randomly.getBoolean()) {
            select.setLimitClause(
                    DatabendConstant.createIntConstant(Randomly.getNotCachedInteger(0, Integer.MAX_VALUE)));
        }
        if (Randomly.getBoolean()) {
            select.setOffsetClause(
                    DatabendConstant.createIntConstant(Randomly.getNotCachedInteger(0, Integer.MAX_VALUE)));
        }

        return select;
    }

}
