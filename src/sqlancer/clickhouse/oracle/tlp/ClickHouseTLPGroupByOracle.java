package sqlancer.clickhouse.oracle.tlp;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import sqlancer.ComparatorHelper;
import sqlancer.clickhouse.ClickHouseProvider;
import sqlancer.clickhouse.ClickHouseVisitor;

public class ClickHouseTLPGroupByOracle extends ClickHouseTLPBase {

    public ClickHouseTLPGroupByOracle(ClickHouseProvider.ClickHouseGlobalState state) {
        super(state);
    }

    @Override
    public String check() throws SQLException {
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
        return "Not implemented!";
    }
}
