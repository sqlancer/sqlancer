package sqlancer.doris.gen;

import sqlancer.Randomly;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.ast.newast.TableReferenceNode;
import sqlancer.doris.ast.DorisConstant;
import sqlancer.doris.ast.DorisExpression;
import sqlancer.doris.ast.DorisJoin;
import sqlancer.doris.ast.DorisSelect;
import sqlancer.doris.DorisProvider.DorisGlobalState;
import sqlancer.doris.DorisSchema.DorisTable;
import sqlancer.doris.DorisSchema.DorisTables;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public final class DorisRandomQuerySynthesizer {

    private DorisRandomQuerySynthesizer() {
    }

    public static DorisSelect generateSelect(DorisGlobalState globalState, int nrColumns) {
        DorisTables targetTables = globalState.getSchema().getRandomTableNonEmptyTables();
        DorisExpressionGenerator gen = new DorisExpressionGenerator(globalState)
                .setColumns(targetTables.getColumns());
        DorisSelect select = new DorisSelect();
        // TODO: distinct
        // select.setDistinct(Randomly.getBoolean());
        // boolean allowAggregates = Randomly.getBooleanWithSmallProbability();
        List<Node<DorisExpression>> columns = new ArrayList<>();
        for (int i = 0; i < nrColumns; i++) {
            // if (allowAggregates && Randomly.getBoolean()) {
            Node<DorisExpression> expression = gen.generateExpression();
            columns.add(expression);
            // } else {
            // columns.add(gen());
            // }
        }
        select.setFetchColumns(columns);
        List<DorisTable> tables = targetTables.getTables();
        List<TableReferenceNode<DorisExpression, DorisTable>> tableList = tables.stream()
                .map(t -> new TableReferenceNode<DorisExpression, DorisTable>(t)).collect(Collectors.toList());
        List<Node<DorisExpression>> joins = DorisJoin.getJoins(tableList, globalState);
        select.setJoinList(joins.stream().collect(Collectors.toList()));
        select.setFromList(tableList.stream().collect(Collectors.toList()));
        if (Randomly.getBoolean()) {
            select.setWhereClause(gen.generateExpression());
        }
        if (Randomly.getBoolean()) {
            select.setOrderByExpressions(gen.generateOrderBys());
        }
        if (Randomly.getBoolean()) {
            select.setGroupByExpressions(gen.generateExpressions(Randomly.smallNumber() + 1));
        }

        if (Randomly.getBoolean()) {
            select.setLimitClause(DorisConstant.createIntConstant(Randomly.getNotCachedInteger(0, Integer.MAX_VALUE)));
        }
        if (Randomly.getBoolean()) {
            select.setOffsetClause(
                    DorisConstant.createIntConstant(Randomly.getNotCachedInteger(0, Integer.MAX_VALUE)));
        }
        if (Randomly.getBoolean()) {
            select.setHavingClause(gen.generateHavingClause());
        }
        return select;
    }

}
