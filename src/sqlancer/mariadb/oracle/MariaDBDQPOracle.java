package sqlancer.mariadb.oracle;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.ComparatorHelper;
import sqlancer.Randomly;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.mariadb.MariaDBErrors;
import sqlancer.mariadb.MariaDBProvider.MariaDBGlobalState;
import sqlancer.mariadb.MariaDBSchema;
import sqlancer.mariadb.MariaDBSchema.MariaDBTables;
import sqlancer.mariadb.ast.MariaDBColumnName;
import sqlancer.mariadb.ast.MariaDBExpression;
import sqlancer.mariadb.ast.MariaDBJoin;
import sqlancer.mariadb.ast.MariaDBSelectStatement;
import sqlancer.mariadb.ast.MariaDBTableReference;
import sqlancer.mariadb.ast.MariaDBVisitor;
import sqlancer.mariadb.gen.MariaDBExpressionGenerator;
import sqlancer.mariadb.gen.MariaDBSetGenerator;

public class MariaDBDQPOracle implements TestOracle<MariaDBGlobalState> {
    private final MariaDBGlobalState state;
    private final MariaDBSchema s;
    private MariaDBExpressionGenerator gen;
    private MariaDBSelectStatement select;
    private final ExpectedErrors errors = new ExpectedErrors();

    public MariaDBDQPOracle(MariaDBGlobalState globalState) {
        state = globalState;
        s = globalState.getSchema();
        MariaDBErrors.addCommonErrors(errors);
    }

    @Override
    public void check() throws Exception {
        MariaDBTables tables = s.getRandomTableNonEmptyTables();
        gen = new MariaDBExpressionGenerator(state.getRandomly()).setColumns(tables.getColumns());

        List<MariaDBExpression> fetchColumns = new ArrayList<>();
        fetchColumns.addAll(Randomly.nonEmptySubset(tables.getColumns()).stream().map(c -> new MariaDBColumnName(c))
                .collect(Collectors.toList()));

        select = new MariaDBSelectStatement();
        select.setFetchColumns(fetchColumns);

        select.setSelectType(Randomly.fromOptions(MariaDBSelectStatement.MariaDBSelectType.values()));
        if (Randomly.getBoolean()) {
            select.setWhereClause(gen.getRandomExpression());
        }
        if (Randomly.getBoolean()) {
            select.setGroupByClause(fetchColumns);
        }

        // Set the join.
        List<MariaDBJoin> joinExpressions = MariaDBJoin.getRandomJoinClauses(tables.getTables(), state.getRandomly());
        select.setJoinClauses(joinExpressions);

        // Set the from clause from the tables that are not used in the join.
        select.setFromList(
                tables.getTables().stream().map(t -> new MariaDBTableReference(t)).collect(Collectors.toList()));

        // Get the result of the first query
        String originalQueryString = MariaDBVisitor.asString(select);
        List<String> originalResult = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors,
                state);

        List<SQLQueryAdapter> optimizationList = MariaDBSetGenerator.getAllOptimizer(state);
        for (SQLQueryAdapter optimization : optimizationList) {
            optimization.execute(state);
            List<String> result = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);
            try {
                ComparatorHelper.assumeResultSetsAreEqual(originalResult, result, originalQueryString,
                        List.of(originalQueryString), state);
            } catch (AssertionError e) {
                String assertionMessage = String.format(
                        "The size of the result sets mismatch (%d and %d)!" + System.lineSeparator()
                                + "First query: \"%s\", whose cardinality is: %d" + System.lineSeparator()
                                + "Second query:\"%s\", whose cardinality is: %d",
                        originalResult.size(), result.size(), originalQueryString, originalResult.size(),
                        String.join(";", originalQueryString), result.size());
                assertionMessage += System.lineSeparator() + "The setting: " + optimization.getQueryString();
                throw new AssertionError(assertionMessage);
            }
        }

    }

}
