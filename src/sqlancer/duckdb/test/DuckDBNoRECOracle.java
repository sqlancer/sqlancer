package sqlancer.duckdb.test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import sqlancer.FoundBugException;
import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.common.ast.newast.ColumnReferenceNode;
import sqlancer.common.ast.newast.NewPostfixTextNode;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.ast.newast.TableReferenceNode;
import sqlancer.common.oracle.NoRECBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.duckdb.DuckDBErrors;
import sqlancer.duckdb.DuckDBOptions;
import sqlancer.duckdb.DuckDBProvider.DuckDBGlobalState;
import sqlancer.duckdb.DuckDBSchema;
import sqlancer.duckdb.DuckDBSchema.DuckDBColumn;
import sqlancer.duckdb.DuckDBSchema.DuckDBCompositeDataType;
import sqlancer.duckdb.DuckDBSchema.DuckDBDataType;
import sqlancer.duckdb.DuckDBSchema.DuckDBTable;
import sqlancer.duckdb.DuckDBSchema.DuckDBTables;
import sqlancer.duckdb.DuckDBToStringVisitor;
import sqlancer.duckdb.ast.DuckDBExpression;
import sqlancer.duckdb.ast.DuckDBJoin;
import sqlancer.duckdb.ast.DuckDBSelect;
import sqlancer.duckdb.gen.DuckDBExpressionGenerator;
import sqlancer.duckdb.gen.DuckDBExpressionGenerator.DuckDBCastOperation;

public class DuckDBNoRECOracle extends NoRECBase<DuckDBGlobalState> implements TestOracle {

    private final int NO_VALID_RESULT = -1;
    private final DuckDBSchema s;

    public DuckDBNoRECOracle(DuckDBGlobalState globalState) {
        super(globalState);
        this.s = globalState.getSchema();
        DuckDBErrors.addExpressionErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        DuckDBTables randomTables = s.getRandomTableNonEmptyTables();
        List<DuckDBColumn> columns = randomTables.getColumns();
        DuckDBExpressionGenerator gen = new DuckDBExpressionGenerator(state).setColumns(columns);
        Node<DuckDBExpression> randomWhereCondition = gen.generateExpression();
        List<DuckDBTable> tables = randomTables.getTables();
        List<TableReferenceNode<DuckDBExpression, DuckDBTable>> tableList = tables.stream()
                .map(t -> new TableReferenceNode<DuckDBExpression, DuckDBTable>(t)).collect(Collectors.toList());
        List<Node<DuckDBExpression>> joins = DuckDBJoin.getJoins(tableList, state);
        Function<DuckDBGlobalState, Integer> unoptimizedQuery = getUnoptimizedQuery(new ArrayList<>(tableList), randomWhereCondition, joins);
        int unoptimizedCount = unoptimizedQuery.apply(state);
        Function<DuckDBGlobalState, Integer> optimizedQuery = getOptimizedQuery(con, new ArrayList<>(tableList), columns,
                randomWhereCondition, joins);
        int optimizedCount = optimizedQuery.apply(state);
        if (optimizedCount == NO_VALID_RESULT || unoptimizedCount == NO_VALID_RESULT) {
            throw new IgnoreMeException();
        }
        if (optimizedCount != unoptimizedCount) {
            state.getState().getLocalState().log(optimizedQueryString + ";\n" + unoptimizedQueryString + ";");
            throw new FoundBugException((optimizedCount + " " + unoptimizedCount),
                    new FoundBugException.Reproducer<DuckDBGlobalState, DuckDBOptions>() {

                        @Override
                        public boolean bugStillTriggers(DuckDBGlobalState globalState) {
                            return optimizedQuery.apply(globalState) != unoptimizedQuery.apply(globalState);
                        }

                        @Override
                        public void outputHook(DuckDBGlobalState globalState) {
                            globalState.getState().logStatement(new SQLQueryAdapter(optimizedQueryString));
                            globalState.getState().logStatement(new SQLQueryAdapter(unoptimizedQueryString));
                        }
                    });
        }

    }

    private Function<DuckDBGlobalState, Integer> getUnoptimizedQuery(List<Node<DuckDBExpression>> tableList,
             Node<DuckDBExpression> randomWhereCondition, List<Node<DuckDBExpression>> joins) throws SQLException {
        DuckDBSelect select = new DuckDBSelect();
        // select.setGroupByClause(groupBys);
        // DuckDBExpression isTrue = DuckDBPostfixOperation.create(randomWhereCondition,
        // PostfixOperator.IS_TRUE);
        Node<DuckDBExpression> asText = new NewPostfixTextNode<>(new DuckDBCastOperation(
                new NewPostfixTextNode<DuckDBExpression>(randomWhereCondition,
                        " IS NOT NULL AND " + DuckDBToStringVisitor.asString(randomWhereCondition)),
                new DuckDBCompositeDataType(DuckDBDataType.INT, 8)), "as count");
        select.setFetchColumns(Arrays.asList(asText));
        select.setFromList(tableList);
        // select.setSelectType(SelectType.ALL);
        select.setJoinList(joins);
        unoptimizedQueryString = "SELECT SUM(count) FROM (" + DuckDBToStringVisitor.asString(select) + ") as res";
        errors.add("canceling statement due to statement timeout");
        SQLQueryAdapter q = new SQLQueryAdapter(unoptimizedQueryString, errors);
        return new Function<DuckDBGlobalState, Integer>() {
            @Override
            public Integer apply(DuckDBGlobalState state) {
                return countRows(q, state);
            }
        };
    }

    private Function<DuckDBGlobalState, Integer> getOptimizedQuery(SQLConnection con, List<Node<DuckDBExpression>> tableList,
            List<DuckDBColumn> columns, Node<DuckDBExpression> randomWhereCondition, List<Node<DuckDBExpression>> joins)
            throws SQLException {
        DuckDBSelect select = new DuckDBSelect();
        // select.setGroupByClause(groupBys);
        // DuckDBAggregate aggr = new DuckDBAggregate(
        List<Node<DuckDBExpression>> allColumns = columns.stream()
                .map((c) -> new ColumnReferenceNode<DuckDBExpression, DuckDBColumn>(c)).collect(Collectors.toList());
        // DuckDBAggregateFunction.COUNT);
        // select.setFetchColumns(Arrays.asList(aggr));
        select.setFetchColumns(allColumns);
        select.setFromList(tableList);
        select.setWhereClause(randomWhereCondition);
        if (Randomly.getBooleanWithSmallProbability()) {
            select.setOrderByExpressions(new DuckDBExpressionGenerator(state).setColumns(columns).generateOrderBys());
        }
        // select.setSelectType(SelectType.ALL);
        select.setJoinList(joins);
        optimizedQueryString = DuckDBToStringVisitor.asString(select);
        if (options.logEachSelect()) {
            logger.writeCurrent(optimizedQueryString);
        }
        SQLQueryAdapter q = new SQLQueryAdapter(optimizedQueryString, errors);
        return new Function<DuckDBGlobalState, Integer>() {
            @Override
            public Integer apply(DuckDBGlobalState state) {
                return countRows(q, state);
            }
        };
    }

    private int countRows(SQLQueryAdapter q, DuckDBGlobalState globalState) {
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
}
