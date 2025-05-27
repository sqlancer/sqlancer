package sqlancer.oxla.oracle;

import sqlancer.ComparatorHelper;
import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.oxla.OxlaGlobalState;
import sqlancer.oxla.ast.OxlaBinaryOperation;
import sqlancer.oxla.ast.OxlaExpression;
import sqlancer.oxla.ast.OxlaOperator;

import java.util.ArrayList;
import java.util.List;

public class OxlaTLPWhereExtendedOracle extends OxlaTLPBase {
    private String originalQueryString;
    private OxlaExpression originalPredicate;

    private static final OxlaOperator andOperator = OxlaBinaryOperation.LOGIC
            .stream()
            .filter(o -> o.textRepresentation.equalsIgnoreCase("and"))
            .findFirst().orElse(null);

    public OxlaTLPWhereExtendedOracle(OxlaGlobalState state, ExpectedErrors errors) {
        super(state);
        this.errors.addAll(errors);
    }

    @Override
    public void check() throws Exception {
        super.check();
        originalPredicate = generatePredicate();
        select.setWhereClause(originalPredicate);
        originalQueryString = select.asString();
        final List<String> originalResult = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);

        final boolean generateOrderBy = Randomly.getBoolean();
        if (generateOrderBy) {
            select.setOrderByClauses(generator.generateOrderBys());
        }
        select.setWhereClause(combinedPredicate(predicate));
        final String trueQuery = select.asString();

        select.setWhereClause(combinedPredicate(negatedPredicate));
        final String falseQuery = select.asString();

        select.setWhereClause(combinedPredicate(isNullPredicate));
        final String isNullQuery = select.asString();

        final List<String> combinedString = new ArrayList<>();
        final List<String> combinedResult = ComparatorHelper.getCombinedResultSetNoDuplicates(trueQuery, falseQuery, isNullQuery, combinedString, !generateOrderBy, state, errors);
        ComparatorHelper.assumeResultSetsAreEqual(originalResult, combinedResult, originalQueryString, combinedString, state);
    }

    @Override
    public String getLastQueryString() {
        return originalQueryString;
    }

    private OxlaExpression combinedPredicate(OxlaExpression expr) {
        assert andOperator != null;
        return new OxlaBinaryOperation(originalPredicate, expr, andOperator);
    }
}
