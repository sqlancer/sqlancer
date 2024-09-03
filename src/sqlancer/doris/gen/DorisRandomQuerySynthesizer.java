package sqlancer.doris.gen;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.doris.DorisProvider.DorisGlobalState;
import sqlancer.doris.DorisSchema;
import sqlancer.doris.DorisSchema.DorisTable;
import sqlancer.doris.DorisSchema.DorisTables;
import sqlancer.doris.ast.DorisColumnValue;
import sqlancer.doris.ast.DorisConstant;
import sqlancer.doris.ast.DorisExpression;
import sqlancer.doris.ast.DorisJoin;
import sqlancer.doris.ast.DorisSelect;
import sqlancer.doris.ast.DorisTableReference;

public final class DorisRandomQuerySynthesizer {

    private DorisRandomQuerySynthesizer() {
    }

    public static DorisSelect generateSelect(DorisGlobalState globalState, int nrColumns) {
        DorisTables targetTables = globalState.getSchema().getRandomTableNonEmptyTables();
        List<DorisSchema.DorisColumn> targetColumns = targetTables.getColumns();
        DorisNewExpressionGenerator gen = new DorisNewExpressionGenerator(globalState).setColumns(targetColumns);
        DorisSelect select = new DorisSelect();
        HashSet<DorisColumnValue> columnOfLeafNode = new HashSet<>();
        gen.setColumnOfLeafNode(columnOfLeafNode);
        int freeColumns = targetColumns.size();
        select.setDistinct(DorisSelect.DorisSelectDistinctType.getRandomWithoutNull());
        List<DorisExpression> columns = new ArrayList<>();
        for (int i = 0; i < nrColumns; i++) {
            DorisExpression column = null;
            if (freeColumns > 0 && Randomly.getBoolean()) {
                column = new DorisColumnValue(targetColumns.get(freeColumns - 1), null);
                freeColumns -= 1;
                columnOfLeafNode.add((DorisColumnValue) column);
            } else {
                column = gen.generateExpression(DorisSchema.DorisDataType.BOOLEAN);
            }
            columns.add(column);
        }
        select.setFetchColumns(columns);
        List<DorisTable> tables = targetTables.getTables();
        List<DorisTableReference> tableList = tables.stream().map(t -> new DorisTableReference(t))
                .collect(Collectors.toList());
        List<DorisJoin> joins = DorisJoin.getJoins(tableList, globalState);
        select.setJoinList(joins.stream().collect(Collectors.toList()));
        select.setFromList(tableList.stream().collect(Collectors.toList()));
        if (Randomly.getBoolean()) {
            select.setHavingClause(gen.generateHavingClause());
        }
        if (Randomly.getBoolean()) {
            select.setWhereClause(gen.generateExpression(DorisSchema.DorisDataType.BOOLEAN));
        }

        List<DorisExpression> noExprColumns = new ArrayList<>(columnOfLeafNode);

        if (Randomly.getBoolean()) {
            select.setOrderByClauses(Randomly.nonEmptySubset(noExprColumns));
        }
        if (Randomly.getBoolean()) {
            select.setGroupByExpressions(noExprColumns);
        }
        if (Randomly.getBoolean()) {
            select.setLimitClause(DorisConstant.createIntConstant(Randomly.getNotCachedInteger(0, Integer.MAX_VALUE)));
        }
        if (Randomly.getBoolean()) {
            select.setOffsetClause(DorisConstant.createIntConstant(Randomly.getNotCachedInteger(0, Integer.MAX_VALUE)));
        }
        return select;
    }

}
