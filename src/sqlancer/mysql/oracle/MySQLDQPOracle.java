package sqlancer.mysql.oracle;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.ComparatorHelper;
import sqlancer.Randomly;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.mysql.MySQLErrors;
import sqlancer.mysql.MySQLGlobalState;
import sqlancer.mysql.MySQLSchema.MySQLTables;
import sqlancer.mysql.MySQLVisitor;
import sqlancer.mysql.ast.MySQLColumnReference;
import sqlancer.mysql.ast.MySQLExpression;
import sqlancer.mysql.ast.MySQLJoin;
import sqlancer.mysql.ast.MySQLSelect;
import sqlancer.mysql.ast.MySQLTableReference;
import sqlancer.mysql.ast.MySQLText;
import sqlancer.mysql.gen.MySQLExpressionGenerator;
import sqlancer.mysql.gen.MySQLHintGenerator;
import sqlancer.mysql.gen.MySQLSetGenerator;

public class MySQLDQPOracle implements TestOracle<MySQLGlobalState> {
    private final MySQLGlobalState state;
    private MySQLExpressionGenerator gen;
    private MySQLSelect select;
    private final ExpectedErrors errors = new ExpectedErrors();

    public MySQLDQPOracle(MySQLGlobalState globalState) {
        state = globalState;
        MySQLErrors.addExpressionErrors(errors);
    }

    @Override
    public void check() throws Exception {
        // Randomly generate a query
        MySQLTables tables = state.getSchema().getRandomTableNonEmptyTables();
        gen = new MySQLExpressionGenerator(state).setColumns(tables.getColumns());
        List<MySQLExpression> fetchColumns = new ArrayList<>();
        fetchColumns.addAll(Randomly.nonEmptySubset(tables.getColumns()).stream()
                .map(c -> new MySQLColumnReference(c, null)).collect(Collectors.toList()));

        select = new MySQLSelect();
        select.setFetchColumns(fetchColumns);

        select.setSelectType(Randomly.fromOptions(MySQLSelect.SelectType.values()));
        if (Randomly.getBoolean()) {
            select.setWhereClause(gen.generateExpression());
        }
        if (Randomly.getBoolean()) {
            select.setGroupByExpressions(fetchColumns);
            if (Randomly.getBoolean()) {
                select.setHavingClause(gen.generateExpression());
            }
        }

        // Set the join.
        List<MySQLJoin> joinExpressions = MySQLJoin.getRandomJoinClauses(tables.getTables(), state);
        select.setJoinList(joinExpressions.stream().map(j -> (MySQLExpression) j).collect(Collectors.toList()));

        // Set the from clause from the tables that are not used in the join.
        List<MySQLExpression> tableList = tables.getTables().stream().map(t -> new MySQLTableReference(t))
                .collect(Collectors.toList());
        select.setFromList(tableList);

        // Get the result of the first query
        String originalQueryString = MySQLVisitor.asString(select);
        List<String> originalResult = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors,
                state);

        // Check hints
        List<MySQLText> hintList = MySQLHintGenerator.generateAllHints(select, tables.getTables());
        for (MySQLText hint : hintList) {
            select.setHint(hint);
            String queryString = MySQLVisitor.asString(select);
            List<String> result = ComparatorHelper.getResultSetFirstColumnAsString(queryString, errors, state);
            ComparatorHelper.assumeResultSetsAreEqual(originalResult, result, originalQueryString, List.of(queryString),
                    state);
        }

        // Check optimizer variables
        List<SQLQueryAdapter> optimizationList = MySQLSetGenerator.getAllOptimizer(state);
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
