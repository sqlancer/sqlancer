package sqlancer.stonedb.oracle;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.ast.newast.ColumnReferenceNode;
import sqlancer.common.ast.newast.NewBinaryOperatorNode;
import sqlancer.common.ast.newast.NewPostfixTextNode;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.ast.newast.TableReferenceNode;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
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

public class StoneDBFuzzOracle implements TestOracle<StoneDBGlobalState> {

    private final StoneDBGlobalState globalState;
    private final StoneDBSchema schema;
    private final ExpectedErrors errors = new ExpectedErrors();

    public StoneDBFuzzOracle(StoneDBGlobalState globalState) {
        this.globalState = globalState;
        this.schema = globalState.getSchema();
        StoneDBErrors.addExpectedExpressionErrors(errors);
    }

    @Override
    public void check() throws Exception {
        StoneDBTables randomTables = schema.getRandomTableNonEmptyTables();
        List<StoneDBColumn> columns = randomTables.getColumns();
        StoneDBExpressionGenerator gen = new StoneDBExpressionGenerator(globalState).setColumns(columns);
        Node<StoneDBExpression> randomWhereCondition = gen.generateExpression();
        List<StoneDBTable> tables = randomTables.getTables();
        List<TableReferenceNode<StoneDBExpression, StoneDBTable>> tableList = tables.stream()
                .map(t -> new TableReferenceNode<StoneDBExpression, StoneDBTable>(t)).collect(Collectors.toList());
        List<Node<StoneDBExpression>> joins = StoneDBJoin.getJoins(tableList, globalState);

        StoneDBSelect select = new StoneDBSelect();
        Node<StoneDBExpression> asText = new NewPostfixTextNode<>(
                new StoneDBCastOperation(
                        new NewBinaryOperatorNode<>(new NewPostfixTextNode<>(randomWhereCondition, " IS NOT NULL "),
                                randomWhereCondition, StoneDBBinaryLogicalOperator.AND),
                        StoneDBDataType.INT),
                " as count");
        select.setFetchColumns(List.of(asText));
        select.setFromList(new ArrayList<>(tableList));
        select.setJoinList(joins);
        String queryString = "SELECT * FROM (" + StoneDBToStringVisitor.asString(select) + ") as res;";
        SQLQueryAdapter q = new SQLQueryAdapter(queryString, errors);
        q.executeAndGetLogged(globalState);

        select = new StoneDBSelect();
        List<Node<StoneDBExpression>> allColumns = columns.stream()
                .map((c) -> new ColumnReferenceNode<StoneDBExpression, StoneDBColumn>(c)).collect(Collectors.toList());
        select.setFetchColumns(allColumns);
        select.setFromList(new ArrayList<>(tableList));
        select.setWhereClause(randomWhereCondition);
        if (Randomly.getBooleanWithSmallProbability()) {
            select.setOrderByExpressions(
                    new StoneDBExpressionGenerator(globalState).setColumns(columns).generateOrderBys());
        }
        select.setJoinList(joins);
        queryString = StoneDBToStringVisitor.asString(select);
        q = new SQLQueryAdapter(queryString, errors);
        q.executeAndGetLogged(globalState);
    }
}
