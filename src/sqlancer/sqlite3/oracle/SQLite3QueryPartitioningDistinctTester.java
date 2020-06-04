package sqlancer.sqlite3.oracle;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import sqlancer.DatabaseProvider;
import sqlancer.TestOracle;
import sqlancer.sqlite3.SQLite3Provider.SQLite3GlobalState;
import sqlancer.sqlite3.SQLite3Visitor;
import sqlancer.sqlite3.ast.SQLite3Select.SelectType;

public class SQLite3QueryPartitioningDistinctTester extends SQLite3QueryPartitioningBase {

    public SQLite3QueryPartitioningDistinctTester(SQLite3GlobalState state) {
        super(state);
    }

    @Override
    public void check() throws SQLException {
        super.check();
        select.setSelectType(SelectType.DISTINCT);
        select.setWhereClause(null);
        String originalQueryString = SQLite3Visitor.asString(select);

        List<String> resultSet = DatabaseProvider.getResultSetFirstColumnAsString(originalQueryString, errors,
                state.getConnection(), state);

        select.setWhereClause(predicate);
        String firstQueryString = SQLite3Visitor.asString(select);
        select.setWhereClause(negatedPredicate);
        String secondQueryString = SQLite3Visitor.asString(select);
        select.setWhereClause(isNullPredicate);
        String thirdQueryString = SQLite3Visitor.asString(select);
        List<String> combinedString = new ArrayList<>();
        List<String> secondResultSet = TestOracle.getCombinedResultSetNoDuplicates(firstQueryString, secondQueryString,
                thirdQueryString, combinedString, true, state, errors);
        TestOracle.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQueryString, combinedString, state);
    }

}
