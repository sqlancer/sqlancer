package sqlancer.hsqldb.test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.common.ast.newast.ColumnReferenceNode;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.ast.newast.TableReferenceNode;
import sqlancer.common.oracle.NoRECBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.hsqldb.HSQLDBErrors;
import sqlancer.hsqldb.HSQLDBProvider.HSQLDBGlobalState;
import sqlancer.hsqldb.HSQLDBSchema;
import sqlancer.hsqldb.HSQLDBSchema.HSQLDBColumn;
import sqlancer.hsqldb.HSQLDBSchema.HSQLDBCompositeDataType;
import sqlancer.hsqldb.HSQLDBSchema.HSQLDBDataType;
import sqlancer.hsqldb.HSQLDBSchema.HSQLDBTable;
import sqlancer.hsqldb.HSQLDBToStringVisitor;
import sqlancer.hsqldb.ast.HSQLDBColumnReference;
import sqlancer.hsqldb.ast.HSQLDBExpression;
import sqlancer.hsqldb.ast.HSQLDBJoin;
import sqlancer.hsqldb.ast.HSQLDBSelect;
import sqlancer.hsqldb.gen.HSQLDBExpressionGenerator;

public class HSQLDBNoRECOracle extends NoRECBase<HSQLDBGlobalState> implements TestOracle {

    private final HSQLDBSchema s;

    public HSQLDBNoRECOracle(HSQLDBGlobalState globalState) {
        super(globalState);
        this.s = globalState.getSchema();
        HSQLDBErrors.addExpressionErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        List<HSQLDBTable> tables = s.getDatabaseTablesRandomSubsetNotEmpty();
        List<HSQLDBColumn> columns = tables.stream().flatMap(t -> t.getColumns().stream()).collect(Collectors.toList());
        HSQLDBExpressionGenerator gen = new HSQLDBExpressionGenerator(state).setColumns(columns);

        Node<HSQLDBExpression> randomWhereCondition = gen
                .generateExpression(HSQLDBCompositeDataType.getRandomWithType(HSQLDBDataType.BOOLEAN));

        List<TableReferenceNode<HSQLDBExpression, HSQLDBTable>> tableList = tables.stream()
                .map(t -> new TableReferenceNode<HSQLDBExpression, HSQLDBTable>(t)).collect(Collectors.toList());
        List<Node<HSQLDBExpression>> joins = HSQLDBJoin.getJoins(tableList, state);
        int secondCount = getSecondQuery(new ArrayList<>(tableList), randomWhereCondition, joins); // 禁用优化
        int firstCount = getFirstQueryCount(con, new ArrayList<>(tableList), columns, randomWhereCondition, joins);
        if (firstCount == -1 || secondCount == -1) {
            throw new IgnoreMeException();
        }
        if (firstCount != secondCount) {
            throw new AssertionError(
                    optimizedQueryString + "; -- " + firstCount + "\n" + unoptimizedQueryString + " -- " + secondCount);
        }
    }

    private int getSecondQuery(List<Node<HSQLDBExpression>> tableList, Node<HSQLDBExpression> randomWhereCondition,
            List<Node<HSQLDBExpression>> joins) throws SQLException {
        HSQLDBSelect select = new HSQLDBSelect();
        HSQLDBColumn c = new HSQLDBColumn("COUNT(*)", null, null);
        select.setFetchColumns(List.of(new HSQLDBColumnReference(c)));
        select.setFromList(tableList);
        select.setWhereClause(randomWhereCondition);
        select.setJoinList(joins);
        int secondCount = 0;
        unoptimizedQueryString = "SELECT SUM(count) FROM (" + HSQLDBToStringVisitor.asString(select) + ") as res";
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

    private int getFirstQueryCount(SQLConnection con, List<Node<HSQLDBExpression>> tableList,
            List<HSQLDBColumn> columns, Node<HSQLDBExpression> randomWhereCondition, List<Node<HSQLDBExpression>> joins)
            throws SQLException {
        HSQLDBSelect select = new HSQLDBSelect();
        List<Node<HSQLDBExpression>> allColumns = columns.stream()
                .map((c) -> new ColumnReferenceNode<HSQLDBExpression, HSQLDBColumn>(c)).collect(Collectors.toList());
        select.setFetchColumns(allColumns);
        select.setFromList(tableList);
        select.setWhereClause(randomWhereCondition);
        if (Randomly.getBooleanWithSmallProbability()) {
            select.setOrderByExpressions(new HSQLDBExpressionGenerator(state).setColumns(columns).generateOrderBys());
        }
        select.setJoinList(joins);
        int firstCount = 0;
        try (Statement stat = con.createStatement()) {
            optimizedQueryString = HSQLDBToStringVisitor.asString(select);
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
