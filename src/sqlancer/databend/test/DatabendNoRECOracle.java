package sqlancer.databend.test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.stream.Collectors;

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
import sqlancer.databend.DatabendExprToNode;
import sqlancer.databend.DatabendProvider.DatabendGlobalState;
import sqlancer.databend.DatabendSchema;
import sqlancer.databend.DatabendSchema.DatabendColumn;
import sqlancer.databend.DatabendSchema.DatabendCompositeDataType;
import sqlancer.databend.DatabendSchema.DatabendDataType;
import sqlancer.databend.DatabendSchema.DatabendTable;
import sqlancer.databend.DatabendSchema.DatabendTables;
import sqlancer.databend.DatabendToStringVisitor;
import sqlancer.databend.ast.DatabendCastOperation;
import sqlancer.databend.ast.DatabendExpression;
import sqlancer.databend.ast.DatabendJoin;
import sqlancer.databend.ast.DatabendSelect;
import sqlancer.databend.gen.DatabendNewExpressionGenerator;

public class DatabendNoRECOracle extends NoRECBase<DatabendGlobalState> implements TestOracle<DatabendGlobalState> {

    private final DatabendSchema s;

    public DatabendNoRECOracle(DatabendGlobalState globalState) {
        super(globalState);
        this.s = globalState.getSchema();
        DatabendErrors.addExpressionErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        DatabendTables randomTables = s.getRandomTableNonEmptyAndViewTables(); // 随机获得nr张表
        List<DatabendColumn> columns = randomTables.getColumns();
        if (columns.isEmpty()) {
            debugColumns(columns, randomTables); // 调试代码，可忽略
        }
        DatabendNewExpressionGenerator gen = new DatabendNewExpressionGenerator(state).setColumns(columns);
        Node<DatabendExpression> randomWhereCondition = DatabendExprToNode
                .cast(gen.generateExpression(DatabendDataType.BOOLEAN)); // 生成随机where条件
        List<DatabendTable> tables = randomTables.getTables();
        List<TableReferenceNode<DatabendExpression, DatabendTable>> tableList = tables.stream()
                .map(t -> new TableReferenceNode<DatabendExpression, DatabendTable>(t)).collect(Collectors.toList());
        List<Node<DatabendExpression>> joins = DatabendJoin.getJoins(tableList, state);
        int secondCount = getUnoptimizedQueryCount(tableList.stream().collect(Collectors.toList()),
                randomWhereCondition, joins);
        int firstCount = getOptimizedQueryCount(con, tableList.stream().collect(Collectors.toList()), columns,
                randomWhereCondition, joins);
        if (firstCount == -1 || secondCount == -1) {
            throw new IgnoreMeException();
        }
        if (firstCount != secondCount) {
            throw new AssertionError(
                    optimizedQueryString + "; -- " + firstCount + "\n" + unoptimizedQueryString + " -- " + secondCount);
        }
    }

    private int getUnoptimizedQueryCount(List<Node<DatabendExpression>> tableList,
            Node<DatabendExpression> randomWhereCondition, List<Node<DatabendExpression>> joins) throws SQLException {
        DatabendSelect select = new DatabendSelect();
        // select.setGroupByClause(groupBys);
        Node<DatabendExpression> asText = new NewPostfixTextNode<>(new DatabendCastOperation(
                new NewPostfixTextNode<DatabendExpression>(randomWhereCondition,
                        " IS NOT NULL AND " + DatabendToStringVisitor.asString(randomWhereCondition)),
                new DatabendCompositeDataType(DatabendDataType.INT, 8)), "as count");

        select.setFetchColumns(List.of(asText));
        select.setFromList(tableList);
        select.setJoinList(joins);
        int secondCount = 0;
        unoptimizedQueryString = "SELECT SUM(count) FROM (" + DatabendToStringVisitor.asString(select) + ") as res";
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

    private int getOptimizedQueryCount(SQLConnection con, List<Node<DatabendExpression>> tableList,
            List<DatabendColumn> columns, Node<DatabendExpression> randomWhereCondition,
            List<Node<DatabendExpression>> joins) throws SQLException {
        DatabendSelect select = new DatabendSelect();
        // select.setGroupByClause(groupBys);
        List<Node<DatabendExpression>> allColumns = columns.stream()
                .map((c) -> new ColumnReferenceNode<DatabendExpression, DatabendColumn>(c))
                .collect(Collectors.toList());
        select.setFetchColumns(allColumns);
        select.setFromList(tableList);
        select.setWhereClause(randomWhereCondition);
        if (Randomly.getBooleanWithSmallProbability()) {
            select.setOrderByExpressions(new DatabendNewExpressionGenerator(state).setColumns(columns)
                    .generateOrderBys().stream().map(DatabendExprToNode::cast).collect(Collectors.toList()));
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

    void debugColumns(List<DatabendColumn> columns, DatabendTables randomTables) {
        DatabendTables test = new DatabendTables(s.getDatabaseTables());
        System.out.println(String.format("tables size: %d", test.getTables().size()));
        for (DatabendTable table : test.getTables()) {
            System.out.println(String.format("%s", table.getName()));
            for (DatabendColumn column : table.getColumns()) {
                System.out.println(String.format("%s %s", column.getName(), column.getType()));
            }
            System.out.println("------------------------");
        }
        System.out.println("+++++++++++++++++++++++++++++");
        for (DatabendTable table : randomTables.getTables()) {
            System.out.println(String.format("%s", table.getName()));
            for (DatabendColumn column : table.getColumns()) {
                System.out.println(String.format("%s %s", column.getName(), column.getType()));
            }
            System.out.println("------------------------");
        }
        throw new AssertionError(
                String.format("randomTables size: %d,column is empty", randomTables.getTables().size()));
    }

}
