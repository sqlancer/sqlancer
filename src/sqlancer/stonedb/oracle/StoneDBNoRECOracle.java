package sqlancer.stonedb.oracle;

import static sqlancer.stonedb.StoneDBBugs.bug1953;

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
import sqlancer.common.ast.newast.NewBinaryOperatorNode;
import sqlancer.common.ast.newast.NewPostfixTextNode;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.ast.newast.TableReferenceNode;
import sqlancer.common.oracle.NoRECBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.stonedb.StoneDBErrors;
import sqlancer.stonedb.StoneDBProvider.StoneDBGlobalState;
import sqlancer.stonedb.StoneDBSchema;
import sqlancer.stonedb.StoneDBSchema.StoneDBColumn;
import sqlancer.stonedb.StoneDBSchema.StoneDBDataType;
import sqlancer.stonedb.StoneDBSchema.StoneDBTable;
import sqlancer.stonedb.StoneDBSchema.StoneDBTables;
import sqlancer.stonedb.StoneDBToStringVisitor;
import sqlancer.stonedb.ast.StoneDBExpression;
import sqlancer.stonedb.ast.StoneDBJoin;
import sqlancer.stonedb.ast.StoneDBSelect;
import sqlancer.stonedb.gen.StoneDBExpressionGenerator;
import sqlancer.stonedb.gen.StoneDBExpressionGenerator.StoneDBBinaryLogicalOperator;
import sqlancer.stonedb.gen.StoneDBExpressionGenerator.StoneDBCastOperation;

public class StoneDBNoRECOracle extends NoRECBase<StoneDBGlobalState> implements TestOracle<StoneDBGlobalState> {

    private final StoneDBSchema schema;

    public StoneDBNoRECOracle(StoneDBGlobalState globalState) {
        super(globalState);
        this.schema = globalState.getSchema();
        StoneDBErrors.addExpectedExpressionErrors(errors);
    }

    @Override
    public void check() throws Exception {
        StoneDBTables randomTables = schema.getRandomTableNonEmptyTables();
        List<StoneDBColumn> columns = randomTables.getColumns();
        StoneDBExpressionGenerator gen = new StoneDBExpressionGenerator(state).setColumns(columns);
        Node<StoneDBExpression> randomWhereCondition = gen.generateExpression();
        List<StoneDBTable> tables = randomTables.getTables();
        List<TableReferenceNode<StoneDBExpression, StoneDBTable>> tableList = tables.stream()
                .map(t -> new TableReferenceNode<StoneDBExpression, StoneDBTable>(t)).collect(Collectors.toList());
        List<Node<StoneDBExpression>> joins = StoneDBJoin.getJoins(tableList, state);
        // get and check count
        int secondCount = getUnoptimizedQueryCount(new ArrayList<>(tableList), randomWhereCondition, joins);
        int firstCount = getOptimizedQueryCount(con, new ArrayList<>(tableList), columns, randomWhereCondition, joins);
        if (firstCount == -1 || secondCount == -1) {
            throw new IgnoreMeException();
        }
        if (firstCount != secondCount) {
            throw new AssertionError(optimizedQueryString + "; -- " + firstCount + System.lineSeparator()
                    + unoptimizedQueryString + " -- " + secondCount);
        }
    }

    private int getUnoptimizedQueryCount(List<Node<StoneDBExpression>> tableList,
            Node<StoneDBExpression> randomWhereCondition, List<Node<StoneDBExpression>> joins) throws SQLException {
        StoneDBSelect select = new StoneDBSelect();
        Node<StoneDBExpression> asText = new NewPostfixTextNode<>(
                new StoneDBCastOperation(
                        new NewBinaryOperatorNode<>(new NewPostfixTextNode<>(randomWhereCondition, " IS NOT NULL "),
                                randomWhereCondition, StoneDBBinaryLogicalOperator.AND),
                        StoneDBDataType.INT),
                " as count");
        select.setFetchColumns(List.of(asText));
        select.setFromList(tableList);
        select.setJoinList(joins);
        unoptimizedQueryString = "SELECT SUM(count) FROM (" + StoneDBToStringVisitor.asString(select) + ") as res;";
        if (bug1953) {
            unoptimizedQueryString = "SELECT * FROM (" + StoneDBToStringVisitor.asString(select) + ") as res;";
        }
        SQLQueryAdapter q = new SQLQueryAdapter(unoptimizedQueryString, errors);
        SQLancerResultSet rs;
        try {
            rs = q.executeAndGetLogged(state);
        } catch (Exception e) {
            throw new AssertionError("error occurred when executing: \"" + unoptimizedQueryString + "\"", e);
        }
        if (rs == null) {
            return -1;
        }
        int secondCount = 0;
        if (bug1953) {
            while (rs.next()) {
                secondCount += rs.getInt(1);
            }
        } else {
            if (rs.next()) {
                secondCount += rs.getLong(1);
            }
        }
        rs.close();
        return secondCount;
    }

    private int getOptimizedQueryCount(SQLConnection con, List<Node<StoneDBExpression>> tableList,
            List<StoneDBColumn> columns, Node<StoneDBExpression> randomWhereCondition,
            List<Node<StoneDBExpression>> joins) {
        StoneDBSelect select = new StoneDBSelect();
        List<Node<StoneDBExpression>> allColumns = columns.stream()
                .map((c) -> new ColumnReferenceNode<StoneDBExpression, StoneDBColumn>(c)).collect(Collectors.toList());
        select.setFetchColumns(allColumns);
        select.setFromList(tableList);
        select.setWhereClause(randomWhereCondition);
        if (Randomly.getBooleanWithSmallProbability()) {
            select.setOrderByExpressions(new StoneDBExpressionGenerator(state).setColumns(columns).generateOrderBys());
        }
        select.setJoinList(joins);
        int firstCount = 0;
        try (Statement stat = con.createStatement()) {
            optimizedQueryString = StoneDBToStringVisitor.asString(select);
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
