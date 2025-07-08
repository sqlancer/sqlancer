package sqlancer.presto.gen;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.presto.PrestoGlobalState;
import sqlancer.presto.PrestoSchema;
import sqlancer.presto.PrestoSchema.PrestoTable;
import sqlancer.presto.PrestoSchema.PrestoTables;
import sqlancer.presto.ast.*;

public final class PrestoRandomQuerySynthesizer {

    private PrestoRandomQuerySynthesizer() {
    }

    public static PrestoSelect generateSelect(PrestoGlobalState globalState, int nrColumns) {
        PrestoTables targetTables = globalState.getSchema().getRandomTableNonEmptyTables();
        PrestoTypedExpressionGenerator gen = new PrestoTypedExpressionGenerator(globalState)
                .setColumns(targetTables.getColumns());
        PrestoSelect select = new PrestoSelect();
        // TODO: distinct
        // select.setDistinct(Randomly.getBoolean());
        // boolean allowAggregates = Randomly.getBooleanWithSmallProbability();
        List<PrestoExpression> columns = new ArrayList<>();
        for (int i = 0; i < nrColumns; i++) {
            // if (allowAggregates && Randomly.getBoolean()) {
            PrestoExpression expression = gen
                    .generateExpression(PrestoSchema.PrestoCompositeDataType.getRandomWithoutNull());
            columns.add(expression);
            // } else {
            // columns.add(gen());
            // }
        }
        columns = gen.aliasSelectFields(columns);
        select.setFetchColumns(columns);
        List<PrestoTable> tables = targetTables.getTables();
        List<PrestoTableReference> tableList = tables.stream().map(t -> new PrestoTableReference(t))
                .collect(Collectors.toList());
        List<PrestoExpression> joins = PrestoJoin.getJoins(tableList, globalState).stream()
                .collect(Collectors.toList());
        select.setJoinList(new ArrayList<>(joins));
        select.setFromList(new ArrayList<>(tableList));
        if (Randomly.getBoolean()) {
            select.setWhereClause(gen.generateBooleanExpression());
        }
        if (Randomly.getBoolean()) {
            select.setOrderByClauses(gen.generateOrderBys());
        }
        if (Randomly.getBoolean()) {
            select.setGroupByExpressions(gen.generateExpressions(Randomly.smallNumber() + 1));
        }

        if (Randomly.getBoolean()) {
            select.setLimitClause(PrestoConstant.createIntConstant(Randomly.getNotCachedInteger(0, Integer.MAX_VALUE)));
        }
        // if (Randomly.getBoolean()) {
        // select.setOffsetClause(
        // PrestoConstant.createIntConstant(Randomly.getNotCachedInteger(0,
        // Integer.MAX_VALUE)));
        // }
        if (Randomly.getBoolean()) {
            select.setHavingClause(gen.generateHavingClause());
        }
        return select;
    }

}
