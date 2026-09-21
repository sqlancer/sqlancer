package sqlancer.clickhouse.oracle;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import sqlancer.ComparatorHelper;
import sqlancer.Randomly;
import sqlancer.clickhouse.ClickHouseErrors;
import sqlancer.clickhouse.ClickHouseProvider.ClickHouseGlobalState;
import sqlancer.clickhouse.ClickHouseSchema;
import sqlancer.clickhouse.ClickHouseSchema.ClickHouseTable;
import sqlancer.clickhouse.ClickHouseVisitor;
import sqlancer.clickhouse.ast.ClickHouseColumnReference;
import sqlancer.clickhouse.ast.ClickHouseExpression;
import sqlancer.clickhouse.ast.ClickHouseSelect;
import sqlancer.clickhouse.ast.ClickHouseTableReference;
import sqlancer.clickhouse.gen.ClickHouseExpressionGenerator;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;

// this class Properly handles NULL values in joins with join_use_nulls = 1
//Disables predicate expression optimization with enable_optimize_predicate_expression = 0
// instead of general class NoRECOracle

public class ClickHouseNoRECOracle implements TestOracle<ClickHouseGlobalState> {

    private final ClickHouseGlobalState state;
    private final ExpectedErrors errors;
    private ClickHouseExpressionGenerator gen;
    private ClickHouseSchema schema;

    public ClickHouseNoRECOracle(ClickHouseGlobalState state) {
        this.state = state;
        this.errors = new ExpectedErrors();
        ClickHouseErrors.addExpectedExpressionErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        schema = state.getSchema();
        gen = new ClickHouseExpressionGenerator(state);

        List<ClickHouseTable> tables = schema.getRandomTableNonEmptyTables().getTables();
        ClickHouseTable table = Randomly.fromList(tables);
        ClickHouseTableReference tableRef = new ClickHouseTableReference(table, null);

        List<ClickHouseColumnReference> columns = tableRef.getColumnReferences();
        gen.addColumns(columns);

        ClickHouseSelect select = new ClickHouseSelect();
        select.setFromClause(tableRef);

        if (state.getClickHouseOptions().testJoins && Randomly.getBoolean()) {
            List<ClickHouseExpression.ClickHouseJoin> joinStatements = gen.getRandomJoinClauses(tableRef, tables);
            columns.addAll(joinStatements.stream().flatMap(j -> j.getRightTable().getColumnReferences().stream())
                    .collect(java.util.stream.Collectors.toList()));
            select.setJoinClauses(joinStatements);
        }

        // Generate predicate, avoiding primary key columns if possible
        List<ClickHouseColumnReference> nonPKColumns = columns.stream()
                .filter(col -> !col.getColumn().isAlias() && !col.getColumn().isMaterialized())
                .collect(java.util.stream.Collectors.toList());

        ClickHouseExpression whereClause;
        if (!nonPKColumns.isEmpty()) {
            whereClause = gen.generateExpressionWithColumns(nonPKColumns, 3);
        } else {
            whereClause = gen.generateExpressionWithColumns(columns, 3);
        }

        select.setWhereClause(whereClause);
        select.setFetchColumns(Arrays.asList(whereClause));
        // Properly handles NULL in JOIN and AGGREGATE
        String query = ClickHouseVisitor.asString(select);
        query += " SETTINGS join_use_nulls = 1, enable_optimize_predicate_expression = 0, aggregate_functions_null_for_empty = 1";

        List<String> firstResult = ComparatorHelper.getResultSetFirstColumnAsString(query, errors, state);
        if (firstResult.isEmpty() || firstResult.get(0).equals("0") || firstResult.get(0).equals("1")) {
            return;
        }

        throw new AssertionError("NoREC faild with result: " + firstResult.get(0) + " for query: " + query);
    }
}
