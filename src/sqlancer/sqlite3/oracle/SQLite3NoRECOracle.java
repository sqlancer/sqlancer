package sqlancer.sqlite3.oracle;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.Reproducer;
import sqlancer.common.oracle.NoRECBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.sqlite3.SQLite3Errors;
import sqlancer.sqlite3.SQLite3GlobalState;
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

public class SQLite3NoRECOracle extends NoRECBase<SQLite3GlobalState> implements TestOracle<SQLite3GlobalState> {

    private static final int NO_VALID_RESULT = -1;
    private final SQLite3Schema s;
    private SQLite3ExpressionGenerator gen;
    private Reproducer<SQLite3GlobalState> reproducer;

    private static class SQLite3NoRECReproducer implements Reproducer<SQLite3GlobalState> {
        private final Function<SQLite3GlobalState, Integer> optimizedQuery;
        private final Function<SQLite3GlobalState, Integer> unoptimizedQuery;

        SQLite3NoRECReproducer(Function<SQLite3GlobalState, Integer> optimizedQuery,
                Function<SQLite3GlobalState, Integer> unoptimizedQuery) {
            this.optimizedQuery = optimizedQuery;
            this.unoptimizedQuery = unoptimizedQuery;
        }

        @Override
        public boolean bugStillTriggers(SQLite3GlobalState globalState) {
            return !Objects.equals(optimizedQuery.apply(globalState), unoptimizedQuery.apply(globalState));
        }
    }

    public SQLite3NoRECOracle(SQLite3GlobalState globalState) {
        super(globalState);
        this.s = globalState.getSchema();
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
        reproducer = null;
        SQLite3Tables randomTables = s.getRandomTableNonEmptyTables();
        List<SQLite3Column> columns = randomTables.getColumns();
        gen = new SQLite3ExpressionGenerator(state).setColumns(columns);
        SQLite3Expression randomWhereCondition = gen.generateExpression();
        List<SQLite3Table> tables = randomTables.getTables();
        List<Join> joinStatements = gen.getRandomJoinClauses(tables);
        List<SQLite3Expression> tableRefs = SQLite3Common.getTableRefs(tables, s);
        SQLite3Select select = new SQLite3Select();
        select.setFromTables(tableRefs);
        select.setJoinClauses(joinStatements);

        Function<SQLite3GlobalState, Integer> optimizedQuery = getOptimizedQuery(select, randomWhereCondition);
        Function<SQLite3GlobalState, Integer> unoptimizedQuery = getUnoptimizedQuery(select, randomWhereCondition);
        int optimizedCount = optimizedQuery.apply(state);
        int unoptimizedCount = unoptimizedQuery.apply(state);
        if (optimizedCount == NO_VALID_RESULT || unoptimizedCount == NO_VALID_RESULT) {
            throw new IgnoreMeException();
        }
        if (optimizedCount != unoptimizedCount) {
            reproducer = new SQLite3NoRECReproducer(optimizedQuery, unoptimizedQuery);
            state.getState().getLocalState().log(optimizedQueryString + ";\n" + unoptimizedQueryString + ";");
            throw new AssertionError(optimizedCount + " " + unoptimizedCount);
        }

    }

    @Override
    public Reproducer<SQLite3GlobalState> getLastReproducer() {
        return reproducer;
    }

    @Override
    public String getLastQueryString() {
        return optimizedQueryString;
    }

    private Function<SQLite3GlobalState, Integer> getUnoptimizedQuery(SQLite3Select select,
            SQLite3Expression randomWhereCondition) throws SQLException {
        SQLite3PostfixUnaryOperation isTrue = new SQLite3PostfixUnaryOperation(PostfixUnaryOperator.IS_TRUE,
                randomWhereCondition);
        SQLite3PostfixText asText = new SQLite3PostfixText(isTrue, " as count", null);
        select.setFetchColumns(Arrays.asList(asText));
        select.setWhereClause(null);
        unoptimizedQueryString = "SELECT SUM(count) FROM (" + SQLite3Visitor.asString(select) + ")";
        if (options.logEachSelect()) {
            logger.writeCurrent(unoptimizedQueryString);
        }
        SQLQueryAdapter q = new SQLQueryAdapter(unoptimizedQueryString, errors);
        return state -> extractCounts(q, state);
    }

    private Function<SQLite3GlobalState, Integer> getOptimizedQuery(SQLite3Select select,
            SQLite3Expression randomWhereCondition) throws SQLException {
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
        optimizedQueryString = SQLite3Visitor.asString(select);
        if (options.logEachSelect()) {
            logger.writeCurrent(optimizedQueryString);
        }
        SQLQueryAdapter q = new SQLQueryAdapter(optimizedQueryString, errors);
        return state -> useAggregate ? extractCounts(q, state) : countRows(q, state);
    }

    private int countRows(SQLQueryAdapter q, SQLite3GlobalState globalState) {
        int count = 0;
        try (SQLancerResultSet rs = q.executeAndGet(globalState)) {
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
            }
        } catch (Exception e) {
            if (e instanceof IgnoreMeException) {
                throw (IgnoreMeException) e;
            }
            throw new AssertionError(unoptimizedQueryString, e);
        }
        return count;
    }

    private int extractCounts(SQLQueryAdapter q, SQLite3GlobalState globalState) {
        int count = 0;
        try (SQLancerResultSet rs = q.executeAndGet(globalState)) {
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
            }
        } catch (Exception e) {
            if (e instanceof IgnoreMeException) {
                throw (IgnoreMeException) e;
            }
            throw new AssertionError(unoptimizedQueryString, e);
        }
        return count;
    }

}
