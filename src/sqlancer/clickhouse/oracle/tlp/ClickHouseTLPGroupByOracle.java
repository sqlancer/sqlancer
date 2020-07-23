package sqlancer.clickhouse.oracle.tlp;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

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
        select.setGroupByClause(select.getFetchColumns());
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
        List<String> secondResultSet = ComparatorHelper.getCombinedResultSetNoDuplicates(firstQueryString,
                secondQueryString, thirdQueryString, combinedString, false, state, errors);
        ComparatorHelper.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQueryString, combinedString,
                state);
    }

    @Override
    List<ClickHouseExpression> generateFetchColumns() {
        List<ClickHouseExpression> columns;
        columns = Randomly.nonEmptySubset(targetTables.getColumns()).stream()
                .map(c -> new ClickHouseColumnReference(c, null)).collect(Collectors.toList());
        return columns;
    }

}
