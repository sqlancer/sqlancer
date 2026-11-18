package sqlancer.common.oracle;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import sqlancer.ComparatorHelper;
import sqlancer.SQLGlobalState;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.schema.AbstractSchema;
import sqlancer.common.schema.AbstractTable;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.simple.clause.Clause;
import sqlancer.simple.clause.Where;
import sqlancer.simple.dialect.Dialect;
import sqlancer.simple.expression.Expression;
import sqlancer.simple.expression.IsNull;
import sqlancer.simple.expression.Not;
import sqlancer.simple.gen.SelectGenerator;
import sqlancer.simple.gen.TLPGenerator;
import sqlancer.simple.statement.Select;

public class TLPWhereOracle2<S extends AbstractSchema<?, T>, T extends AbstractTable<C, ?, ?>, C extends AbstractTableColumn<?, ?>, G extends SQLGlobalState<?, S>>
        implements TestOracle<G> {

    private final Dialect dialect;

    private final G state;

    private final ExpectedErrors errors;

    public TLPWhereOracle2(G state, Dialect dialect, ExpectedErrors expectedErrors) {
        if (state == null || dialect == null || expectedErrors == null) {
            throw new IllegalArgumentException("Null variables used to initialize test oracle.");
        }
        this.dialect = dialect;
        this.state = state;
        this.errors = expectedErrors;
    }

    @Override
    public void check() throws SQLException {
        S s = state.getSchema();
        List<T> targetTables = TestOracleUtils.getRandomTableNonEmptyTables(s).getTables();

        TLPGenerator gen = new SelectGenerator<>(dialect, targetTables, state.getRandomly(),
                state.getOptions().getMaxExpressionDepth());
        Select select = gen.generateSelect();

        select.setClause(new Where.Empty());

        String originalQueryString = select.print();
        List<String> firstResultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors,
                state);

        Expression predicate = gen.generateExpression();

        Clause whereClause = dialect.map(new Where.Of(predicate));
        select.setClause(whereClause);
        String firstQueryString = select.print();

        whereClause = dialect.map(new Where.Of(dialect.map(new Not(predicate))));
        select.setClause(whereClause);
        String secondQueryString = select.print();

        whereClause = dialect.map(new Where.Of(dialect.map(new IsNull(predicate, false))));
        select.setClause(whereClause);
        String thirdQueryString = select.print();

        List<String> combinedString = new ArrayList<>();
        List<String> secondResultSet = ComparatorHelper.getCombinedResultSet(firstQueryString, secondQueryString,
                thirdQueryString, combinedString, true, state, errors);

        ComparatorHelper.assumeResultSetsAreEqual(firstResultSet, secondResultSet, originalQueryString, combinedString,
                state);
    }
}
