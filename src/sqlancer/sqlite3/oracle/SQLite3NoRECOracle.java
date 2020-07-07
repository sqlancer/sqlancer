package sqlancer.sqlite3.oracle;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import sqlancer.IgnoreMeException;
import sqlancer.Main.StateLogger;
import sqlancer.MainOptions;
import sqlancer.QueryAdapter;
import sqlancer.Randomly;
import sqlancer.StateToReproduce.SQLite3StateToReproduce;
import sqlancer.TestOracle;
import sqlancer.sqlite3.SQLite3Errors;
import sqlancer.sqlite3.SQLite3Provider.SQLite3GlobalState;
import sqlancer.sqlite3.SQLite3Visitor;
import sqlancer.sqlite3.ast.SQLite3Aggregate;
import sqlancer.sqlite3.ast.SQLite3Expression;
import sqlancer.sqlite3.ast.SQLite3Expression.Join;
import sqlancer.sqlite3.ast.SQLite3Expression.SQLite3ColumnName;
import sqlancer.sqlite3.ast.SQLite3Expression.SQLite3PostfixText;
import sqlancer.sqlite3.ast.SQLite3Expression.SQLite3PostfixUnaryOperation;
import sqlancer.sqlite3.ast.SQLite3Expression.SQLite3PostfixUnaryOperation.PostfixUnaryOperator;
import sqlancer.sqlite3.ast.SQLite3Select;
import sqlancer.sqlite3.gen.SQLite3Common;
import sqlancer.sqlite3.gen.SQLite3ExpressionGenerator;
import sqlancer.sqlite3.schema.SQLite3Schema;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Column;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Table;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Tables;

public class SQLite3NoRECOracle implements TestOracle {

    private static final int NO_VALID_RESULT = -1;
    private final SQLite3Schema s;
    private final SQLite3StateToReproduce state;
    private final Set<String> errors = new HashSet<>();
    private final StateLogger logger;
    private final MainOptions options;
    private final SQLite3GlobalState globalState;
    private SQLite3ExpressionGenerator gen;
    private String firstQueryString;
    private String secondQueryString;

    public SQLite3NoRECOracle(SQLite3GlobalState globalState) {
        this.s = globalState.getSchema();
        this.state = (SQLite3StateToReproduce) globalState.getState();
        this.logger = globalState.getLogger();
        this.options = globalState.getOptions();
        this.globalState = globalState;
        SQLite3Errors.addExpectedExpressionErrors(errors);
        SQLite3Errors.addMatchQueryErrors(errors);
        SQLite3Errors.addQueryErrors(errors);
        errors.add("misuse of aggregate");
        errors.add("misuse of window function");
        errors.add("second argument to nth_value must be a positive integer");
        errors.add("no such table");
        errors.add("no query solution");
        errors.add("unable to use function MATCH in the requested context");
    }

    @Override
    public void check() throws SQLException {
        SQLite3Tables randomTables = s.getRandomTableNonEmptyTables();
        List<SQLite3Column> columns = randomTables.getColumns();
        gen = new SQLite3ExpressionGenerator(globalState).setColumns(columns);
        SQLite3Expression randomWhereCondition = gen.generateExpression();
        List<SQLite3Table> tables = randomTables.getTables();
        List<Join> joinStatements = gen.getRandomJoinClauses(tables);
        List<SQLite3Expression> tableRefs = SQLite3Common.getTableRefs(tables, s);
        SQLite3Select select = new SQLite3Select();
        select.setFromTables(tableRefs);
        select.setJoinClauses(joinStatements);

        int optimizedCount = getOptimizedQuery(select, randomWhereCondition);
        int unoptimizedCount = getUnoptimizedQuery(select, randomWhereCondition);
        if (optimizedCount == NO_VALID_RESULT || unoptimizedCount == NO_VALID_RESULT) {
            throw new IgnoreMeException();
        }
        if (optimizedCount != unoptimizedCount) {
            state.queryString = firstQueryString + ";\n" + secondQueryString + ";";
            throw new AssertionError(optimizedCount + " " + unoptimizedCount);
        }

    }

    private int getUnoptimizedQuery(SQLite3Select select, SQLite3Expression randomWhereCondition) throws SQLException {
        SQLite3PostfixUnaryOperation isTrue = new SQLite3PostfixUnaryOperation(PostfixUnaryOperator.IS_TRUE,
                randomWhereCondition);
        SQLite3PostfixText asText = new SQLite3PostfixText(isTrue, " as count", null);
        select.setFetchColumns(Arrays.asList(asText));
        select.setWhereClause(null);
        secondQueryString = "SELECT SUM(count) FROM (" + SQLite3Visitor.asString(select) + ")";
        if (options.logEachSelect()) {
            logger.writeCurrent(secondQueryString);
        }
        QueryAdapter q = new QueryAdapter(secondQueryString, errors);
        return extractCounts(q);
    }

    private int getOptimizedQuery(SQLite3Select select, SQLite3Expression randomWhereCondition) throws SQLException {
        boolean useAggregate = Randomly.getBoolean();
        if (Randomly.getBoolean()) {
            select.setOrderByExpressions(gen.generateOrderBys());
        }
        if (useAggregate) {
            select.setFetchColumns(Arrays.asList(new SQLite3Aggregate(Collections.emptyList(),
                    SQLite3Aggregate.SQLite3AggregateFunction.COUNT_ALL)));
        } else {
            SQLite3ColumnName aggr = new SQLite3ColumnName(SQLite3Column.createDummy("*"), null);
            select.setFetchColumns(Arrays.asList(aggr));
        }
        select.setWhereClause(randomWhereCondition);
        firstQueryString = SQLite3Visitor.asString(select);
        if (options.logEachSelect()) {
            logger.writeCurrent(firstQueryString);
        }
        QueryAdapter q = new QueryAdapter(firstQueryString, errors);
        return useAggregate ? extractCounts(q) : countRows(q);
    }

    private int countRows(QueryAdapter q) {
        int count = 0;
        try (ResultSet rs = q.executeAndGet(globalState)) {
            if (rs == null) {
                return NO_VALID_RESULT;
            } else {
                try {
                    while (rs.next()) {
                        count++;
                    }
                } catch (SQLException e) {
                    count = NO_VALID_RESULT;
                }
                rs.getStatement().close();
            }
        } catch (Exception e) {
            if (e instanceof IgnoreMeException) {
                throw (IgnoreMeException) e;
            }
            throw new AssertionError(secondQueryString, e);
        }
        return count;
    }

    private int extractCounts(QueryAdapter q) {
        int count = 0;
        try (ResultSet rs = q.executeAndGet(globalState)) {
            if (rs == null) {
                return NO_VALID_RESULT;
            } else {
                try {
                    while (rs.next()) {
                        count += rs.getInt(1);
                    }
                } catch (SQLException e) {
                    count = NO_VALID_RESULT;
                }
                rs.getStatement().close();
            }
        } catch (Exception e) {
            if (e instanceof IgnoreMeException) {
                throw (IgnoreMeException) e;
            }
            throw new AssertionError(secondQueryString, e);
        }
        return count;
    }

}
