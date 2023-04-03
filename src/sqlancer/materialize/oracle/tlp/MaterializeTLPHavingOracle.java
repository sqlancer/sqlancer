package sqlancer.materialize.oracle.tlp;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import sqlancer.ComparatorHelper;
import sqlancer.Randomly;
import sqlancer.materialize.MaterializeGlobalState;
import sqlancer.materialize.MaterializeSchema.MaterializeDataType;
import sqlancer.materialize.MaterializeVisitor;
import sqlancer.materialize.ast.MaterializeExpression;
import sqlancer.materialize.gen.MaterializeCommon;

public class MaterializeTLPHavingOracle extends MaterializeTLPBase {
    private String generatedQueryString;

    public MaterializeTLPHavingOracle(MaterializeGlobalState state) {
        super(state);
        MaterializeCommon.addGroupingErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        super.check();
        havingCheck();
    }

    protected void havingCheck() throws SQLException {
        if (Randomly.getBoolean()) {
            select.setWhereClause(gen.generateExpression(MaterializeDataType.BOOLEAN));
        }
        select.setGroupByExpressions(gen.generateExpressions(Randomly.smallNumber() + 1));
        select.setHavingClause(null);
        String originalQueryString = MaterializeVisitor.asString(select);
        generatedQueryString = originalQueryString;
        List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);

        // See https://github.com/MaterializeInc/materialize/issues/18346, have to check if predicate errors by putting
        // it in SELECT first
        List<MaterializeExpression> originalColumns = select.getFetchColumns();
        List<MaterializeExpression> checkColumns = new ArrayList<>();
        checkColumns.add(predicate);
        select.setFetchColumns(checkColumns);
        String errorCheckQueryString = MaterializeVisitor.asString(select);
        ComparatorHelper.getResultSetFirstColumnAsString(errorCheckQueryString, errors, state);
        select.setFetchColumns(originalColumns);

        boolean orderBy = Randomly.getBoolean();
        if (orderBy) {
            select.setOrderByExpressions(gen.generateOrderBy());
        }
        select.setHavingClause(predicate);
        String firstQueryString = MaterializeVisitor.asString(select);
        select.setHavingClause(negatedPredicate);
        String secondQueryString = MaterializeVisitor.asString(select);
        select.setHavingClause(isNullPredicate);
        String thirdQueryString = MaterializeVisitor.asString(select);
        List<String> combinedString = new ArrayList<>();
        List<String> secondResultSet = ComparatorHelper.getCombinedResultSet(firstQueryString, secondQueryString,
                thirdQueryString, combinedString, !orderBy, state, errors);
        ComparatorHelper.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQueryString, combinedString,
                state);
    }

    @Override
    protected MaterializeExpression generatePredicate() {
        return gen.generateHavingClause();
    }

    @Override
    List<MaterializeExpression> generateFetchColumns() {
        List<MaterializeExpression> expressions = gen.allowAggregates(true)
                .generateExpressions(Randomly.smallNumber() + 1);
        gen.allowAggregates(false);
        return expressions;
    }

    @Override
    public String getLastQueryString() {
        return generatedQueryString;
    }
}
