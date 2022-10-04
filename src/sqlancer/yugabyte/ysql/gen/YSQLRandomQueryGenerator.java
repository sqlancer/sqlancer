package sqlancer.yugabyte.ysql.gen;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.yugabyte.ysql.YSQLGlobalState;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLDataType;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLTables;
import sqlancer.yugabyte.ysql.ast.YSQLConstant;
import sqlancer.yugabyte.ysql.ast.YSQLExpression;
import sqlancer.yugabyte.ysql.ast.YSQLSelect;
import sqlancer.yugabyte.ysql.ast.YSQLSelect.ForClause;
import sqlancer.yugabyte.ysql.ast.YSQLSelect.SelectType;
import sqlancer.yugabyte.ysql.ast.YSQLSelect.YSQLFromTable;

public final class YSQLRandomQueryGenerator {

    private YSQLRandomQueryGenerator() {
    }

    public static YSQLSelect createRandomQuery(int nrColumns, YSQLGlobalState globalState) {
        List<YSQLExpression> columns = new ArrayList<>();
        YSQLTables tables = globalState.getSchema().getRandomTableNonEmptyTables();
        YSQLExpressionGenerator gen = new YSQLExpressionGenerator(globalState).setColumns(tables.getColumns());
        for (int i = 0; i < nrColumns; i++) {
            columns.add(gen.generateExpression(0));
        }
        YSQLSelect select = new YSQLSelect();
        select.setSelectType(SelectType.getRandom());
        if (select.getSelectOption() == SelectType.DISTINCT && Randomly.getBoolean()) {
            select.setDistinctOnClause(gen.generateExpression(0));
        }
        select.setFromList(tables.getTables().stream().map(t -> new YSQLFromTable(t, Randomly.getBoolean()))
                .collect(Collectors.toList()));
        select.setFetchColumns(columns);
        if (Randomly.getBoolean()) {
            select.setWhereClause(gen.generateExpression(0, YSQLDataType.BOOLEAN));
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
            select.setLimitClause(YSQLConstant.createIntConstant(Randomly.getPositiveOrZeroNonCachedInteger()));
            if (Randomly.getBoolean()) {
                select.setOffsetClause(YSQLConstant.createIntConstant(Randomly.getPositiveOrZeroNonCachedInteger()));
            }
        }
        if (Randomly.getBooleanWithRatherLowProbability()) {
            select.setForClause(ForClause.getRandom());
        }
        return select;
    }

}
