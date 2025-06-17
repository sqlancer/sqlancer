package sqlancer.duckdb.gen;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.duckdb.DuckDBProvider.DuckDBGlobalState;
import sqlancer.duckdb.DuckDBSchema.DuckDBTable;
import sqlancer.duckdb.DuckDBSchema.DuckDBTables;
import sqlancer.duckdb.ast.DuckDBConstant;
import sqlancer.duckdb.ast.DuckDBExpression;
import sqlancer.duckdb.ast.DuckDBJoin;
import sqlancer.duckdb.ast.DuckDBSelect;
import sqlancer.duckdb.ast.DuckDBTableReference;

public final class DuckDBRandomQuerySynthesizer {

    private DuckDBRandomQuerySynthesizer() {
    }

    public static DuckDBSelect generateSelect(DuckDBGlobalState globalState, int nrColumns) {
        DuckDBTables targetTables = globalState.getSchema().getRandomTableNonEmptyTables();
        DuckDBExpressionGenerator gen = new DuckDBExpressionGenerator(globalState)
                .setColumns(targetTables.getColumns());
        DuckDBSelect select = new DuckDBSelect();
        // TODO: distinct
        // select.setDistinct(Randomly.getBoolean());
        // boolean allowAggregates = Randomly.getBooleanWithSmallProbability();
        List<DuckDBExpression> columns = new ArrayList<>();
        for (int i = 0; i < nrColumns; i++) {
            // if (allowAggregates && Randomly.getBoolean()) {
            DuckDBExpression expression = gen.generateExpression();
            columns.add(expression);
            // } else {
            // columns.add(gen());
            // }
        }
        select.setFetchColumns(columns);
        List<DuckDBTable> tables = targetTables.getTables();
        List<DuckDBTableReference> tableList = tables.stream().map(t -> new DuckDBTableReference(t))
                .collect(Collectors.toList());
        List<DuckDBJoin> joins = DuckDBJoin.getJoins(tableList, globalState);
        select.setJoinList(joins.stream().collect(Collectors.toList()));
        select.setFromList(tableList.stream().collect(Collectors.toList()));
        if (Randomly.getBoolean()) {
            select.setWhereClause(gen.generateExpression());
        }
        if (Randomly.getBoolean()) {
            select.setOrderByClauses(gen.generateOrderBys());
        }
        if (Randomly.getBoolean()) {
            select.setGroupByExpressions(gen.generateExpressions(Randomly.smallNumber() + 1));
        }

        if (Randomly.getBoolean()) {
            select.setLimitClause(DuckDBConstant
                    .createIntConstant(BigInteger.valueOf(Randomly.getNotCachedInteger(0, Integer.MAX_VALUE))));
        }
        if (Randomly.getBoolean()) {
            select.setOffsetClause(DuckDBConstant
                    .createIntConstant(BigInteger.valueOf(Randomly.getNotCachedInteger(0, Integer.MAX_VALUE))));
        }
        if (Randomly.getBoolean()) {
            select.setHavingClause(gen.generateHavingClause());
        }
        return select;
    }

}
