package sqlancer.doris.oracle.tlp;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import sqlancer.ComparatorHelper;
import sqlancer.doris.DorisErrors;
import sqlancer.doris.DorisProvider.DorisGlobalState;
import sqlancer.doris.visitor.DorisToStringVisitor;

public class DorisQueryPartitioningDistinctTester extends DorisQueryPartitioningBase {

    public DorisQueryPartitioningDistinctTester(DorisGlobalState state) {
        super(state);
        DorisErrors.addExpressionErrors(errors);
        DorisErrors.addInsertErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        super.check();
        select.setDistinct(true);
        select.setWhereClause(null);
        String originalQueryString = DorisToStringVisitor.asString(select);
        List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);

        List<String> queryStrings = getQueryStrings(select);
        String firstQueryString = queryStrings.get(0);
        String secondQueryString = queryStrings.get(1);
        String thirdQueryString = queryStrings.get(2);
        List<String> combinedString = new ArrayList<>();

        String unionString = "SELECT DISTINCT * FROM (" + firstQueryString + " UNION ALL " + secondQueryString
                + " UNION ALL " + thirdQueryString + ") tmpTable";
        combinedString.add(unionString);
        List<String> secondResultSet = ComparatorHelper.getResultSetFirstColumnAsString(unionString, errors, state);
        ComparatorHelper.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQueryString, combinedString,
                state, ComparatorHelper::canonicalizeResultValue);
    }

}
