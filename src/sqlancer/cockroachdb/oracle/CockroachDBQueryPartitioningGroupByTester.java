package sqlancer.cockroachdb.oracle;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.DatabaseProvider;
import sqlancer.Randomly;
import sqlancer.TestOracle;
import sqlancer.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;
import sqlancer.cockroachdb.CockroachDBVisitor;
import sqlancer.cockroachdb.ast.CockroachDBColumnReference;
import sqlancer.cockroachdb.ast.CockroachDBExpression;

public class CockroachDBQueryPartitioningGroupByTester extends CockroachDBQueryPartitioningBase {

    public CockroachDBQueryPartitioningGroupByTester(CockroachDBGlobalState state) {
        super(state);
    }

    @Override
    public void check() throws SQLException {
        super.check();
        select.setGroupByExpressions(select.getFetchColumns());
        select.setWhereClause(null);
        String originalQueryString = CockroachDBVisitor.asString(select);

        List<String> resultSet = DatabaseProvider.getResultSetFirstColumnAsString(originalQueryString, errors,
                state.getConnection(), state);

        select.setWhereClause(predicate);
        String firstQueryString = CockroachDBVisitor.asString(select);
        select.setWhereClause(negatedPredicate);
        String secondQueryString = CockroachDBVisitor.asString(select);
        select.setWhereClause(isNullPredicate);
        String thirdQueryString = CockroachDBVisitor.asString(select);
        List<String> combinedString = new ArrayList<>();
        List<String> secondResultSet = TestOracle.getCombinedResultSetNoDuplicates(firstQueryString, secondQueryString,
                thirdQueryString, combinedString, true, state, errors);
        TestOracle.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQueryString, combinedString, state);
    }

    @Override
    List<CockroachDBExpression> generateFetchColumns() {
        List<CockroachDBExpression> columns = new ArrayList<>();
        columns = Randomly.nonEmptySubset(targetTables.getColumns().stream().map(c -> new CockroachDBColumnReference(c))
                .collect(Collectors.toList()));
        return columns;
    }

}
