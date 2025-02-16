package sqlancer.clickhouse.oracle.tlp;

import static java.lang.Math.min;
import static java.util.stream.IntStream.range;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.ComparatorHelper;
import sqlancer.Randomly;
import sqlancer.clickhouse.ClickHouseErrors;
import sqlancer.clickhouse.ClickHouseProvider.ClickHouseGlobalState;
import sqlancer.clickhouse.ClickHouseSchema;
import sqlancer.clickhouse.ClickHouseSchema.ClickHouseTable;
import sqlancer.clickhouse.ClickHouseVisitor;
import sqlancer.clickhouse.ast.ClickHouseColumnReference;
import sqlancer.clickhouse.ast.ClickHouseExpression;
import sqlancer.clickhouse.ast.ClickHouseExpression.ClickHouseJoin;
import sqlancer.clickhouse.ast.ClickHouseSelect;
import sqlancer.clickhouse.ast.ClickHouseTableReference;
import sqlancer.clickhouse.gen.ClickHouseExpressionGenerator;
import sqlancer.common.gen.ExpressionGenerator;
import sqlancer.common.oracle.TernaryLogicPartitioningOracleBase;
import sqlancer.common.oracle.TestOracle;

public class ClickHouseTLPBase extends TernaryLogicPartitioningOracleBase<ClickHouseExpression, ClickHouseGlobalState>
        implements TestOracle<ClickHouseGlobalState> {

    ClickHouseSchema schema;
    List<ClickHouseColumnReference> columns;
    ClickHouseExpressionGenerator gen;
    ClickHouseSelect select;

    public ClickHouseTLPBase(ClickHouseGlobalState state) {
        super(state);
        ClickHouseErrors.addExpectedExpressionErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        gen = new ClickHouseExpressionGenerator(state);
        schema = state.getSchema();
        select = new ClickHouseSelect();
        List<ClickHouseTable> tables = schema.getRandomTableNonEmptyTables().getTables();
        ClickHouseTableReference table = new ClickHouseTableReference(
                tables.get((int) Randomly.getNotCachedInteger(0, tables.size())),
                Randomly.getBoolean() ? "left" : null);
        select.setFromClause(table);
        columns = table.getColumnReferences();

        if (state.getClickHouseOptions().testJoins && Randomly.getBoolean()) {
            List<ClickHouseJoin> joinStatements = gen.getRandomJoinClauses(table, tables);
            columns.addAll(joinStatements.stream().flatMap(j -> j.getRightTable().getColumnReferences().stream())
                    .collect(Collectors.toList()));
            select.setJoinClauses(joinStatements);
        }
        gen.addColumns(columns);
        int small = Randomly.smallNumber();
        List<ClickHouseExpression> from = range(0, 1 + small)
                .mapToObj(i -> gen.generateExpressionWithColumns(columns, 5)).collect(Collectors.toList());
        select.setFetchColumns(from);
        select.setWhereClause(null);
        initializeTernaryPredicateVariants();
        // Smoke check
        String query = ClickHouseVisitor.asString(select);
        ComparatorHelper.getResultSetFirstColumnAsString(query, errors, state);
    }

    List<ClickHouseExpression> generateFetchColumns(List<ClickHouseColumnReference> columns) {
        List<ClickHouseColumnReference> list = Randomly.extractNrRandomColumns(columns,
                min(1 + Randomly.smallNumber(), columns.size()));
        return list.stream().map(c -> (ClickHouseExpression) c).collect(Collectors.toList());
    }

    @Override
    protected ExpressionGenerator<ClickHouseExpression> getGen() {
        return gen;
    }

    protected void executeAndCompare(ClickHouseSelect select, boolean allowDuplicates) throws SQLException {
        select.setWhereClause(null);
        String originalQueryString = ClickHouseVisitor.asString(select);

        List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);

        select.setWhereClause(predicate);
        String firstQueryString = ClickHouseVisitor.asString(select);
        select.setWhereClause(negatedPredicate);
        String secondQueryString = ClickHouseVisitor.asString(select);
        select.setWhereClause(isNullPredicate);
        String thirdQueryString = ClickHouseVisitor.asString(select);
        List<String> combinedString = new ArrayList<>();
        List<String> secondResultSet = allowDuplicates
                ? ComparatorHelper.getCombinedResultSet(firstQueryString, secondQueryString, thirdQueryString,
                        combinedString, true, state, errors)
                : ComparatorHelper.getCombinedResultSetNoDuplicates(firstQueryString, secondQueryString,
                        thirdQueryString, combinedString, false, state, errors);
        ComparatorHelper.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQueryString, combinedString,
                state);
    }

}
