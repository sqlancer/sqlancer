package sqlancer.cockroachdb.oracle.tlp;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import sqlancer.DatabaseProvider;
import sqlancer.TestOracle;
import sqlancer.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBDataType;
import sqlancer.cockroachdb.CockroachDBVisitor;
import sqlancer.cockroachdb.ast.CockroachDBExpression;
import sqlancer.cockroachdb.ast.CockroachDBNotOperation;
import sqlancer.cockroachdb.ast.CockroachDBUnaryPostfixOperation;
import sqlancer.cockroachdb.ast.CockroachDBUnaryPostfixOperation.CockroachDBUnaryPostfixOperator;

public class CockroachDBTLPDistinctOracle extends CockroachDBTLPBase {

    public CockroachDBTLPDistinctOracle(CockroachDBGlobalState state) {
        super(state);
        errors.add("GROUP BY term out of range");
    }

    @Override
    public void check() throws SQLException {
        super.check();
        select.setDistinct(true);
        String originalQueryString = CockroachDBVisitor.asString(select);

        List<String> resultSet = DatabaseProvider.getResultSetFirstColumnAsString(originalQueryString, errors,
                state.getConnection(), state);
        select.setDistinct(false);
        CockroachDBExpression predicate = gen.generateExpression(CockroachDBDataType.BOOL.get());
        select.setWhereClause(predicate);
        String firstQueryString = CockroachDBVisitor.asString(select);
        select.setWhereClause(new CockroachDBNotOperation(predicate));
        String secondQueryString = CockroachDBVisitor.asString(select);
        select.setWhereClause(new CockroachDBUnaryPostfixOperation(predicate, CockroachDBUnaryPostfixOperator.IS_NULL));
        String thirdQueryString = CockroachDBVisitor.asString(select);
        List<String> combinedString = new ArrayList<>();
        List<String> secondResultSet = TestOracle.getCombinedResultSetNoDuplicates(firstQueryString, secondQueryString,
                thirdQueryString, combinedString, true, state, errors);
        TestOracle.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQueryString, combinedString, state);
    }
}
