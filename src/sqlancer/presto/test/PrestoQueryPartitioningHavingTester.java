package sqlancer.presto.test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import sqlancer.ComparatorHelper;
import sqlancer.Randomly;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.oracle.TestOracle;
import sqlancer.presto.PrestoErrors;
import sqlancer.presto.PrestoGlobalState;
import sqlancer.presto.PrestoSchema;
import sqlancer.presto.PrestoToStringVisitor;
import sqlancer.presto.ast.PrestoExpression;

public class PrestoQueryPartitioningHavingTester extends PrestoQueryPartitioningBase
        implements TestOracle<PrestoGlobalState> {

    public PrestoQueryPartitioningHavingTester(PrestoGlobalState state) {
        super(state);
        PrestoErrors.addGroupByErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        super.check();
        if (Randomly.getBoolean()) {
            select.setWhereClause(gen.generateExpression(PrestoSchema.PrestoCompositeDataType.getRandomWithoutNull()));
        }
        boolean orderBy = Randomly.getBoolean();
        if (orderBy) {
            select.setOrderByExpressions(gen.generateOrderBys());
        }
        select.setGroupByExpressions(gen.generateExpressions(Randomly.smallNumber() + 1));
        select.setHavingClause(null);
        String originalQueryString = PrestoToStringVisitor.asString(select);
        List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);

        select.setHavingClause(predicate);
        String firstQueryString = PrestoToStringVisitor.asString(select);
        select.setHavingClause(negatedPredicate);
        String secondQueryString = PrestoToStringVisitor.asString(select);
        select.setHavingClause(isNullPredicate);
        String thirdQueryString = PrestoToStringVisitor.asString(select);
        List<String> combinedString = new ArrayList<>();
        List<String> secondResultSet = ComparatorHelper.getCombinedResultSet(firstQueryString, secondQueryString,
                thirdQueryString, combinedString, !orderBy, state, errors);
        ComparatorHelper.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQueryString, combinedString,
                state, PrestoQueryPartitioningBase::canonicalizeResultValue);
    }

    @Override
    protected Node<PrestoExpression> generatePredicate() {
        return gen.generateHavingClause();
    }

    @Override
    List<Node<PrestoExpression>> generateFetchColumns() {
        return Collections.singletonList(gen.generateHavingClause());
    }

}
