package sqlancer.mysql.gen;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.mysql.MySQLGlobalState;
import sqlancer.mysql.MySQLSchema.MySQLTables;
import sqlancer.mysql.ast.MySQLAggregate;
import sqlancer.mysql.ast.MySQLConstant;
import sqlancer.mysql.ast.MySQLExpression;
import sqlancer.mysql.ast.MySQLSelect;
import sqlancer.mysql.ast.MySQLTableReference;

public final class MySQLRandomQuerySynthesizer {

    private MySQLRandomQuerySynthesizer() {
    }

    public static MySQLSelect generate(MySQLGlobalState globalState, int nrColumns) {
        MySQLTables tables = globalState.getSchema().getRandomTableNonEmptyTables();
        MySQLExpressionGenerator gen = new MySQLExpressionGenerator(globalState).setColumns(tables.getColumns());
        MySQLSelect select = new MySQLSelect();

        List<MySQLExpression> allColumns = new ArrayList<>();
        List<MySQLExpression> columnsWithoutAggregations = new ArrayList<>();

        boolean hasGeneratedAggregate = false;
        boolean hasGeneratedWindowFunction = false;

        select.setSelectType(Randomly.fromOptions(MySQLSelect.SelectType.values()));
        for (int i = 0; i < nrColumns; i++) {
            if (Randomly.getBoolean()) {
                MySQLExpression expression = gen.generateExpression();
                allColumns.add(expression);
                columnsWithoutAggregations.add(expression);
            } else {
                MySQLAggregate aggregate = gen.generateAggregate();
                if (Randomly.getBoolean() && !hasGeneratedWindowFunction) {
                    aggregate.setWindowSpecification(generateWindowSpecification());
                    hasGeneratedWindowFunction = true;
                }
                allColumns.add(aggregate);
                hasGeneratedAggregate = true;
            }
        }
        select.setFetchColumns(allColumns);

        List<MySQLExpression> tableList = tables.getTables().stream().map(t -> new MySQLTableReference(t))
                .collect(Collectors.toList());
        select.setFromList(tableList);
        if (Randomly.getBoolean()) {
            select.setWhereClause(gen.generateExpression());
        }
        if (Randomly.getBooleanWithRatherLowProbability()) {
            select.setOrderByClauses(gen.generateOrderBys());
        }
        if (hasGeneratedAggregate || Randomly.getBoolean()) {
            select.setGroupByExpressions(columnsWithoutAggregations);
            if (Randomly.getBoolean()) {
                select.setHavingClause(gen.generateHavingClause());
            }
        }
        if (Randomly.getBoolean()) {
            select.setLimitClause(MySQLConstant.createIntConstant(Randomly.getPositiveOrZeroNonCachedInteger()));
            if (Randomly.getBoolean()) {
                select.setOffsetClause(MySQLConstant.createIntConstant(Randomly.getPositiveOrZeroNonCachedInteger()));
            }
        }
        return select;
    }

    private static String generateWindowSpecification() {
        StringBuilder sb = new StringBuilder();
        sb.append("OVER (");

        if (Randomly.getBoolean()) {
            sb.append("PARTITION BY ");
        }

        if (Randomly.getBoolean()) {
            if (sb.length() > 6) {
                sb.append(" ");
            }
            sb.append("ORDER BY ");
        }

        if (Randomly.getBoolean()) {
            if (sb.length() > 6) {
                sb.append(" ");
            }
            sb.append(Randomly.fromOptions("ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW",
                    "ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING",
                    "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW",
                    "RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING"));
        }

        sb.append(")");
        return sb.toString();
    }

}
