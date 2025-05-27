package sqlancer.oxla.oracle;

import sqlancer.ComparatorHelper;
import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.oxla.OxlaGlobalState;

import java.util.ArrayList;
import java.util.List;

public class OxlaTLPHavingOracle extends OxlaTLPBase {
    public OxlaTLPHavingOracle(OxlaGlobalState state, ExpectedErrors errors) {
        super(state);
        this.errors.addAll(errors);
    }

    @Override
    public void check() throws Exception {
        super.check();
        if (Randomly.getBoolean()) {
            select.setWhereClause(generator.generatePredicate());
        }
        final boolean generateOrderBy = Randomly.getBoolean();
        if (generateOrderBy) {
            select.setOrderByClauses(generator.generateOrderBys());
        }
        select.setGroupByExpressions(generator.generateExpressions(Randomly.smallNumber() + 1));
        select.setHavingClause(null);

        final String originalQuery = select.asString();
        final List<String> originalResult = ComparatorHelper.getResultSetFirstColumnAsString(originalQuery, errors, state);

        select.setHavingClause(predicate);
        final String trueQuery = select.asString();

        select.setHavingClause(negatedPredicate);
        final String falseQuery = select.asString();

        select.setHavingClause(isNullPredicate);
        final String isNullQuery = select.asString();

        final List<String> combinedString = new ArrayList<>();
        final List<String> combinedResult = ComparatorHelper.getCombinedResultSetNoDuplicates(trueQuery, falseQuery, isNullQuery, combinedString, !generateOrderBy, state, errors);
        ComparatorHelper.assumeResultSetsAreEqual(originalResult, combinedResult, originalQuery, combinedString, state);
    }
}
