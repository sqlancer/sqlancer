package sqlancer.duckdb.test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.Main.StateLogger;
import sqlancer.MainOptions;
import sqlancer.Query;
import sqlancer.QueryAdapter;
import sqlancer.Randomly;
import sqlancer.TestOracle;
import sqlancer.ast.newast.ColumnReferenceNode;
import sqlancer.ast.newast.NewPostfixTextNode;
import sqlancer.ast.newast.Node;
import sqlancer.ast.newast.TableReferenceNode;
import sqlancer.duckdb.DuckDBErrors;
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

public class DuckDBNoRECOracle implements TestOracle {

    private final DuckDBSchema s;
    private final Connection con;
    private String firstQueryString;
    private String secondQueryString;
    private final StateLogger logger;
    private final MainOptions options;
    private final Set<String> errors = new HashSet<>();
    private final DuckDBGlobalState globalState;

    public DuckDBNoRECOracle(DuckDBGlobalState globalState) {
        this.s = globalState.getSchema();
        this.con = globalState.getConnection();
        this.logger = globalState.getLogger();
        this.options = globalState.getOptions();
        this.globalState = globalState;
        DuckDBErrors.addExpressionErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        // DuckDBCommon.addCommonExpressionErrors(errors);
        // DuckDBCommon.addCommonFetchErrors(errors);
        DuckDBTables randomTables = s.getRandomTableNonEmptyTables();
        List<DuckDBColumn> columns = randomTables.getColumns();
        DuckDBExpressionGenerator gen = new DuckDBExpressionGenerator(globalState).setColumns(columns);
        Node<DuckDBExpression> randomWhereCondition = gen.generateExpression();
        List<DuckDBTable> tables = randomTables.getTables();
        List<TableReferenceNode<DuckDBExpression, DuckDBTable>> tableList = tables.stream()
                .map(t -> new TableReferenceNode<DuckDBExpression, DuckDBTable>(t)).collect(Collectors.toList());
        List<Node<DuckDBExpression>> joins = DuckDBJoin.getJoins(tableList, globalState);
        int secondCount = getSecondQuery(tableList.stream().collect(Collectors.toList()), randomWhereCondition, joins);
        int firstCount = getFirstQueryCount(con, tableList.stream().collect(Collectors.toList()), columns,
                randomWhereCondition, joins);
        if (firstCount == -1 || secondCount == -1) {
            throw new IgnoreMeException();
        }
        if (firstCount != secondCount) {
            throw new AssertionError(
                    firstQueryString + "; -- " + firstCount + "\n" + secondQueryString + " -- " + secondCount);
        }
    }

    private int getSecondQuery(List<Node<DuckDBExpression>> tableList, Node<DuckDBExpression> randomWhereCondition,
            List<Node<DuckDBExpression>> joins) throws SQLException {
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
        int secondCount = 0;
        secondQueryString = "SELECT SUM(count) FROM (" + DuckDBToStringVisitor.asString(select) + ") as res";
        errors.add("canceling statement due to statement timeout");
        Query q = new QueryAdapter(secondQueryString, errors);
        ResultSet rs;
        try {
            rs = q.executeAndGetLogged(globalState);
        } catch (Exception e) {
            throw new AssertionError(secondQueryString, e);
        }
        if (rs == null) {
            return -1;
        }
        if (rs.next()) {
            secondCount += rs.getLong(1);
        }
        rs.close();
        return secondCount;
    }

    private int getFirstQueryCount(Connection con, List<Node<DuckDBExpression>> tableList, List<DuckDBColumn> columns,
            Node<DuckDBExpression> randomWhereCondition, List<Node<DuckDBExpression>> joins) throws SQLException {
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
            select.setOrderByExpressions(
                    new DuckDBExpressionGenerator(globalState).setColumns(columns).generateOrderBys());
        }
        // select.setSelectType(SelectType.ALL);
        select.setJoinList(joins);
        int firstCount = 0;
        try (Statement stat = con.createStatement()) {
            firstQueryString = DuckDBToStringVisitor.asString(select);
            if (options.logEachSelect()) {
                logger.writeCurrent(firstQueryString);
            }
            try (ResultSet rs = stat.executeQuery(firstQueryString)) {
                while (rs.next()) {
                    firstCount++;
                }
            }
        } catch (SQLException e) {
            throw new IgnoreMeException();
        }
        return firstCount;
    }

}
