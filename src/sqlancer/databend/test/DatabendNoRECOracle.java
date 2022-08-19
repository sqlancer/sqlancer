package sqlancer.databend.test;

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
import sqlancer.databend.DatabendErrors;
import sqlancer.databend.DatabendSchema;
import sqlancer.databend.DatabendToStringVisitor;
import sqlancer.databend.ast.DatabendExpression;
import sqlancer.databend.ast.DatabendJoin;
import sqlancer.databend.ast.DatabendSelect;
import sqlancer.databend.gen.DatabendExpressionGenerator;
import sqlancer.databend.DatabendProvider.DatabendGlobalState;
import sqlancer.databend.DatabendSchema.*;
import sqlancer.databend.gen.DatabendExpressionGenerator.DatabendCastOperation;
import sqlancer.databend.gen.DatabendNoRECExpressionGenerator;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class DatabendNoRECOracle extends NoRECBase<DatabendGlobalState> implements TestOracle {

    private final DatabendSchema s;

    public DatabendNoRECOracle(DatabendGlobalState globalState) {
        super(globalState);
        this.s = globalState.getSchema();
        DatabendErrors.addExpressionErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        DatabendTables randomTables = s.getRandomTableNonEmptyTables(); //随机获得nr张表
        List<DatabendColumn> columns = randomTables.getColumns();
//        DatabendExpressionGenerator gen = new DatabendExpressionGenerator(state).setColumns(columns);
        DatabendNoRECExpressionGenerator gen = new DatabendNoRECExpressionGenerator(state).setColumns(columns);

        Node<DatabendExpression> randomWhereCondition = gen.generateExpression(DatabendDataType.BOOLEAN); //生成随机where条件，形式为ast

//        System.out.println(DatabendToStringVisitor.asString(randomWhereCondition));

        List<DatabendTable> tables = randomTables.getTables();
        List<TableReferenceNode<DatabendExpression, DatabendTable>> tableList = tables.stream()
                .map(t -> new TableReferenceNode<DatabendExpression, DatabendTable>(t)).collect(Collectors.toList());
        List<Node<DatabendExpression>> joins = DatabendJoin.getJoins(tableList, state);
        int secondCount = getSecondQuery(tableList.stream().collect(Collectors.toList()), randomWhereCondition, joins); //禁用优化
        int firstCount = getFirstQueryCount(con, tableList.stream().collect(Collectors.toList()), columns,
                randomWhereCondition, joins);
        if (firstCount == -1 || secondCount == -1) {
            throw new IgnoreMeException();
        }
        if (firstCount != secondCount) {
            throw new AssertionError(
                    optimizedQueryString + "; -- " + firstCount + "\n" + unoptimizedQueryString + " -- " + secondCount);
        }
    }

    private int getSecondQuery(List<Node<DatabendExpression>> tableList, Node<DatabendExpression> randomWhereCondition,
                               List<Node<DatabendExpression>> joins) throws SQLException {
        DatabendSelect select = new DatabendSelect();
        // select.setGroupByClause(groupBys);
        // DatabendExpression isTrue = DatabendPostfixOperation.create(randomWhereCondition,
        // PostfixOperator.IS_TRUE);
        Node<DatabendExpression> asText = new NewPostfixTextNode<>(new DatabendCastOperation(
                new NewPostfixTextNode<DatabendExpression>(randomWhereCondition,
                        " IS NOT NULL AND " + DatabendToStringVisitor.asString(randomWhereCondition)),
                new DatabendCompositeDataType(DatabendDataType.INT, 8)), "as count");

        select.setFetchColumns(Arrays.asList(asText)); // ?
        select.setFromList(tableList);
        select.setJoinList(joins);
        int secondCount = 0;
        unoptimizedQueryString = "SELECT SUM(count) FROM (" + DatabendToStringVisitor.asString(select) + ") as res";
        errors.add("canceling statement due to statement timeout");
//        System.out.println(unoptimizedQueryString);
        SQLQueryAdapter q = new SQLQueryAdapter(unoptimizedQueryString, errors);
        SQLancerResultSet rs;
        try {
            rs = q.executeAndGetLogged(state);
        } catch (Exception e) {
            throw new AssertionError(unoptimizedQueryString, e);
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

    private int getFirstQueryCount(SQLConnection con, List<Node<DatabendExpression>> tableList,
                                   List<DatabendColumn> columns, Node<DatabendExpression> randomWhereCondition, List<Node<DatabendExpression>> joins)
            throws SQLException {
        DatabendSelect select = new DatabendSelect();
        // select.setGroupByClause(groupBys);
        // DatabendAggregate aggr = new DatabendAggregate(
        List<Node<DatabendExpression>> allColumns = columns.stream()
                .map((c) -> new ColumnReferenceNode<DatabendExpression, DatabendColumn>(c)).collect(Collectors.toList());
        // DatabendAggregateFunction.COUNT);
        // select.setFetchColumns(Arrays.asList(aggr));
        select.setFetchColumns(allColumns);
        select.setFromList(tableList);
        select.setWhereClause(randomWhereCondition);
        if (Randomly.getBooleanWithSmallProbability()) {
            select.setOrderByExpressions(new DatabendExpressionGenerator(state).setColumns(columns).generateOrderBys());
        }
        select.setJoinList(joins);
        int firstCount = 0;
        try (Statement stat = con.createStatement()) {
            optimizedQueryString = DatabendToStringVisitor.asString(select);
            if (options.logEachSelect()) {
                logger.writeCurrent(optimizedQueryString);
            }
            try (ResultSet rs = stat.executeQuery(optimizedQueryString)) {
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
