package sqlancer.materialize.gen;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.materialize.MaterializeGlobalState;
import sqlancer.materialize.MaterializeSchema.MaterializeDataType;
import sqlancer.materialize.MaterializeSchema.MaterializeTables;
import sqlancer.materialize.ast.MaterializeConstant;
import sqlancer.materialize.ast.MaterializeExpression;
import sqlancer.materialize.ast.MaterializeSelect;
import sqlancer.materialize.ast.MaterializeSelect.ForClause;
import sqlancer.materialize.ast.MaterializeSelect.MaterializeFromTable;
import sqlancer.materialize.ast.MaterializeSelect.SelectType;

public final class MaterializeRandomQueryGenerator {

    private MaterializeRandomQueryGenerator() {
    }

    public static MaterializeSelect createRandomQuery(int nrColumns, MaterializeGlobalState globalState) {
        List<MaterializeExpression> columns = new ArrayList<>();
        MaterializeTables tables = globalState.getSchema().getRandomTableNonEmptyTables();
        MaterializeExpressionGenerator gen = new MaterializeExpressionGenerator(globalState)
                .setColumns(tables.getColumns());
        for (int i = 0; i < nrColumns; i++) {
            columns.add(gen.generateExpression(0));
        }
        MaterializeSelect select = new MaterializeSelect();
        select.setSelectType(SelectType.getRandom());
        if (select.getSelectOption() == SelectType.DISTINCT && Randomly.getBoolean()) {
            select.setDistinctOnClause(gen.generateExpression(0));
        }
        select.setFromList(tables.getTables().stream().map(t -> new MaterializeFromTable(t, Randomly.getBoolean()))
                .collect(Collectors.toList()));
        select.setFetchColumns(columns);
        if (Randomly.getBoolean()) {
            select.setWhereClause(gen.generateExpression(0, MaterializeDataType.BOOLEAN));
        }
        if (Randomly.getBooleanWithRatherLowProbability()) {
            select.setGroupByExpressions(gen.generateExpressions(Randomly.smallNumber() + 1));
            if (Randomly.getBoolean()) {
                select.setHavingClause(gen.generateHavingClause());
            }
        }
        if (Randomly.getBooleanWithRatherLowProbability()) {
            select.setOrderByExpressions(gen.generateOrderBy());
        }
        if (Randomly.getBoolean()) {
            select.setLimitClause(MaterializeConstant.createIntConstant(Randomly.getPositiveOrZeroNonCachedInteger()));
            if (Randomly.getBoolean()) {
                select.setOffsetClause(
                        MaterializeConstant.createIntConstant(Randomly.getPositiveOrZeroNonCachedInteger()));
            }
        }
        if (Randomly.getBooleanWithRatherLowProbability()) {
            select.setForClause(ForClause.getRandom());
        }
        return select;
    }

}
