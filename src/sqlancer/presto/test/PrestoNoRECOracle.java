package sqlancer.presto.test;

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
import sqlancer.common.ast.newast.NewPostfixTextNode;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.ast.newast.TableReferenceNode;
import sqlancer.common.oracle.NoRECBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.presto.PrestoErrors;
import sqlancer.presto.PrestoGlobalState;
import sqlancer.presto.PrestoSchema;
import sqlancer.presto.PrestoSchema.PrestoColumn;
import sqlancer.presto.PrestoSchema.PrestoCompositeDataType;
import sqlancer.presto.PrestoSchema.PrestoDataType;
import sqlancer.presto.PrestoSchema.PrestoTable;
import sqlancer.presto.PrestoSchema.PrestoTables;
import sqlancer.presto.PrestoToStringVisitor;
import sqlancer.presto.ast.PrestoCastFunction;
import sqlancer.presto.ast.PrestoExpression;
import sqlancer.presto.ast.PrestoJoin;
import sqlancer.presto.ast.PrestoSelect;
import sqlancer.presto.gen.PrestoTypedExpressionGenerator;

public class PrestoNoRECOracle extends NoRECBase<PrestoGlobalState> implements TestOracle<PrestoGlobalState> {

    private final PrestoSchema s;

    public PrestoNoRECOracle(PrestoGlobalState globalState) {
        super(globalState);
        this.s = globalState.getSchema();
        PrestoErrors.addExpressionErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        PrestoTables randomTables = s.getRandomTableNonEmptyTables();
        List<PrestoColumn> columns = randomTables.getColumns();

        List<PrestoTable> tables = randomTables.getTables();

        List<TableReferenceNode<PrestoExpression, PrestoTable>> tableList = tables.stream()
                .map(t -> new TableReferenceNode<PrestoExpression, PrestoTable>(t)).collect(Collectors.toList());
        List<Node<PrestoExpression>> joins = PrestoJoin.getJoins(tableList, state);

        PrestoTypedExpressionGenerator gen = new PrestoTypedExpressionGenerator(state).setColumns(columns);
        Node<PrestoExpression> randomWhereCondition = gen.generatePredicate();
        int secondCount = getSecondQuery(new ArrayList<>(tableList), randomWhereCondition, joins);

        int firstCount = getFirstQueryCount(con, new ArrayList<>(tableList), columns, randomWhereCondition, joins);
        if (firstCount == -1 || secondCount == -1) {
            throw new IgnoreMeException();
        }
        if (firstCount != secondCount) {
            throw new AssertionError(
                    optimizedQueryString + "; -- " + firstCount + "\n" + unoptimizedQueryString + " -- " + secondCount);
        }
    }

    private int getSecondQuery(List<Node<PrestoExpression>> tableList, Node<PrestoExpression> randomWhereCondition,
            List<Node<PrestoExpression>> joins) throws SQLException {
        PrestoSelect select = new PrestoSelect();

        Node<PrestoExpression> asText = new NewPostfixTextNode<>(

                new PrestoCastFunction(
                        new NewPostfixTextNode<>(randomWhereCondition,
                                " IS NOT NULL AND " + PrestoToStringVisitor.asString(randomWhereCondition)),
                        new PrestoCompositeDataType(PrestoDataType.INT, 8, 0)),
                "as count");

        select.setFetchColumns(List.of(asText));
        select.setFromList(tableList);
        select.setJoinList(joins);
        int secondCount = 0;
        unoptimizedQueryString = "SELECT SUM(count) FROM (" + PrestoToStringVisitor.asString(select) + ") as res";

        errors.add("canceling statement due to statement timeout");
        SQLQueryAdapter q = new SQLQueryAdapter(unoptimizedQueryString, errors, false, false);
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

    private int getFirstQueryCount(SQLConnection con, List<Node<PrestoExpression>> tableList,
            List<PrestoColumn> columns, Node<PrestoExpression> randomWhereCondition,
            List<Node<PrestoExpression>> joins) {
        PrestoSelect select = new PrestoSelect();
        List<Node<PrestoExpression>> allColumns = columns.stream()
                .map((c) -> new ColumnReferenceNode<PrestoExpression, PrestoColumn>(c)).collect(Collectors.toList());
        select.setFetchColumns(allColumns);
        select.setFromList(tableList);
        select.setWhereClause(randomWhereCondition);
        if (Randomly.getBooleanWithSmallProbability()) {
            select.setOrderByExpressions(
                    new PrestoTypedExpressionGenerator(state).setColumns(columns).generateOrderBys());
        }
        select.setJoinList(joins);
        int firstCount = 0;
        try (Statement stat = con.createStatement()) {
            optimizedQueryString = PrestoToStringVisitor.asString(select);
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
