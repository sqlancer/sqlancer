package sqlancer.clickhouse.oracle.tlp;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import sqlancer.ComparatorHelper;
import sqlancer.Randomly;
import sqlancer.clickhouse.ClickHouseProvider;
import sqlancer.clickhouse.ClickHouseVisitor;
import sqlancer.clickhouse.ast.ClickHouseColumnReference;
import sqlancer.clickhouse.ast.ClickHouseExpression;

public class ClickHouseTLPGroupByOracle extends ClickHouseTLPBase {

    public ClickHouseTLPGroupByOracle(ClickHouseProvider.ClickHouseGlobalState state) {
        super(state);
    }

    @Override
    public void check() throws SQLException {
        super.check();

        // avoiding primary key columns if possible in groub by
        List<ClickHouseColumnReference> nonPKColumns = columns.stream()
                .filter(col -> !col.getColumn().isAlias() && !col.getColumn().isMaterialized())
                .collect(Collectors.toList());

        List<ClickHouseExpression> groupByColumns;
        if (!nonPKColumns.isEmpty()) {
            groupByColumns = IntStream.range(0, 1 + Randomly.smallNumber())
                    .mapToObj(i -> gen.generateExpressionWithColumns(nonPKColumns, 5)).collect(Collectors.toList());
        } else {
            groupByColumns = IntStream.range(0, 1 + Randomly.smallNumber())
                    .mapToObj(i -> gen.generateExpressionWithColumns(columns, 5)).collect(Collectors.toList());
        }

        select.setGroupByClause(groupByColumns);
        select.setWhereClause(null);
        String originalQueryString = ClickHouseVisitor.asString(select);
        originalQueryString += " SETTINGS join_use_nulls = 1";

        List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);

        select.setWhereClause(predicate);
        String firstQueryString = ClickHouseVisitor.asString(select);
        select.setWhereClause(negatedPredicate);
        String secondQueryString = ClickHouseVisitor.asString(select);
        select.setWhereClause(isNullPredicate);
        String thirdQueryString = ClickHouseVisitor.asString(select);
        List<String> combinedString = new ArrayList<>();
        List<String> secondResultSet = ComparatorHelper.getCombinedResultSet(
                firstQueryString + " SETTINGS join_use_nulls = 1", secondQueryString + " SETTINGS join_use_nulls = 1",
                thirdQueryString + " SETTINGS join_use_nulls = 1", combinedString, true, state, errors);
        ComparatorHelper.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQueryString, combinedString,
                state);
    }
}
