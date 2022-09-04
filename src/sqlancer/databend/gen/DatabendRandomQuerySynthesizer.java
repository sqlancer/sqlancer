package sqlancer.databend.gen;

import sqlancer.Randomly;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.ast.newast.TableReferenceNode;
import sqlancer.databend.DatabendSchema;
import sqlancer.databend.ast.DatabendConstant;
import sqlancer.databend.ast.DatabendExpression;
import sqlancer.databend.ast.DatabendJoin;
import sqlancer.databend.ast.DatabendSelect;
import sqlancer.databend.DatabendProvider.DatabendGlobalState;
import sqlancer.databend.DatabendSchema.DatabendTable;
import sqlancer.databend.DatabendSchema.DatabendTables;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public final class DatabendRandomQuerySynthesizer {

    private DatabendRandomQuerySynthesizer() {
    }

    public static DatabendSelect generateSelect(DatabendGlobalState globalState, int nrColumns) {
        DatabendTables targetTables = globalState.getSchema().getRandomTableNonEmptyTables();
//        DatabendExpressionGenerator gen = new DatabendExpressionGenerator(globalState)
//                .setColumns(targetTables.getColumns());
        DatabendNewExpressionGenerator gen = new DatabendNewExpressionGenerator(globalState)
                .setColumns(targetTables.getColumns());
        DatabendSelect select = new DatabendSelect();
        // TODO distinct
        // select.setDistinct(Randomly.getBoolean());
        // boolean allowAggregates = Randomly.getBooleanWithSmallProbability();
        List<Node<DatabendExpression>> columns = new ArrayList<>();
        for (int i = 0; i < nrColumns; i++) {
            // if (allowAggregates && Randomly.getBoolean()) {
            Node<DatabendExpression> expression = gen.generateExpression(DatabendSchema.DatabendDataType.BOOLEAN);
            columns.add(expression);
            // } else {
            // columns.add(gen());
            // }
        }
        select.setFetchColumns(columns);
        List<DatabendTable> tables = targetTables.getTables();
        List<TableReferenceNode<DatabendExpression, DatabendTable>> tableList = tables.stream()
                .map(t -> new TableReferenceNode<DatabendExpression, DatabendTable>(t)).collect(Collectors.toList());
        List<Node<DatabendExpression>> joins = DatabendJoin.getJoins(tableList, globalState);
        select.setJoinList(joins.stream().collect(Collectors.toList()));
        select.setFromList(tableList.stream().collect(Collectors.toList()));
        if (Randomly.getBoolean()) {
            select.setWhereClause(gen.generateExpression(DatabendSchema.DatabendDataType.BOOLEAN));
        }
        if (Randomly.getBoolean()) {
            select.setOrderByExpressions(gen.generateOrderBys());
        }
        if (Randomly.getBoolean()) {
            select.setGroupByExpressions(gen.generateExpressions(Randomly.smallNumber() + 1));
        }

        if (Randomly.getBoolean()) {
            select.setLimitClause(DatabendConstant.createIntConstant(Randomly.getNotCachedInteger(0, Integer.MAX_VALUE)));
        }
        if (Randomly.getBoolean()) {
            select.setOffsetClause(
                    DatabendConstant.createIntConstant(Randomly.getNotCachedInteger(0, Integer.MAX_VALUE)));
        }
        // TODO 待添加HavingClause
//        if (Randomly.getBoolean()) {
//            select.setHavingClause(gen.generateHavingClause());
//        }
        return select;
    }

}
