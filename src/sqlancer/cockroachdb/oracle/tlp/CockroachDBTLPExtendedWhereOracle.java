package sqlancer.cockroachdb.oracle.tlp;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import sqlancer.DatabaseProvider;
import sqlancer.Randomly;
import sqlancer.TestOracle;
import sqlancer.cockroachdb.CockroachDBErrors;
import sqlancer.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;
import sqlancer.cockroachdb.CockroachDBVisitor;
import sqlancer.cockroachdb.ast.CockroachDBBinaryLogicalOperation;
import sqlancer.cockroachdb.ast.CockroachDBBinaryLogicalOperation.CockroachDBBinaryLogicalOperator;
import sqlancer.cockroachdb.ast.CockroachDBExpression;
import sqlancer.cockroachdb.ast.CockroachDBNotOperation;
import sqlancer.cockroachdb.ast.CockroachDBUnaryPostfixOperation;
import sqlancer.cockroachdb.ast.CockroachDBUnaryPostfixOperation.CockroachDBUnaryPostfixOperator;

public class CockroachDBTLPExtendedWhereOracle extends CockroachDBTLPBase {

    private CockroachDBExpression originalPredicate;

    public CockroachDBTLPExtendedWhereOracle(CockroachDBGlobalState state) {
        super(state);
        CockroachDBErrors.addExpressionErrors(errors);
        errors.add("GROUP BY term out of range");
    }

    @Override
    public void check() throws SQLException {
        super.check();
        originalPredicate = generatePredicate();
        select.setWhereClause(originalPredicate);
        String originalQueryString = CockroachDBVisitor.asString(select);
        List<String> resultSet = DatabaseProvider.getResultSetFirstColumnAsString(originalQueryString, errors,
                state.getConnection(), state);

        boolean allowOrderBy = Randomly.getBoolean();
        if (allowOrderBy) {
            select.setOrderByExpressions(gen.getOrderingTerms());
        }
        select.setWhereClause(combinePredicate(predicate));
        String firstQueryString = CockroachDBVisitor.asString(select);
        select.setWhereClause(combinePredicate(new CockroachDBNotOperation(predicate)));
        String secondQueryString = CockroachDBVisitor.asString(select);
        select.setWhereClause(combinePredicate(
                new CockroachDBUnaryPostfixOperation(predicate, CockroachDBUnaryPostfixOperator.IS_NULL)));
        String thirdQueryString = CockroachDBVisitor.asString(select);
        List<String> combinedString = new ArrayList<>();
        List<String> secondResultSet = TestOracle.getCombinedResultSet(firstQueryString, secondQueryString,
                thirdQueryString, combinedString, !allowOrderBy, state, errors);
        TestOracle.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQueryString, combinedString, state);
    }

    public CockroachDBExpression combinePredicate(CockroachDBExpression expr) {
        return new CockroachDBBinaryLogicalOperation(originalPredicate, expr, CockroachDBBinaryLogicalOperator.AND);

    }
}
