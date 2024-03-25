package sqlancer.stonedb.gen;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.ast.newast.ColumnReferenceNode;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.ast.newast.TableReferenceNode;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.stonedb.StoneDBProvider.StoneDBGlobalState;
import sqlancer.stonedb.StoneDBSchema;
import sqlancer.stonedb.StoneDBSchema.StoneDBColumn;
import sqlancer.stonedb.StoneDBSchema.StoneDBTable;
import sqlancer.stonedb.StoneDBSchema.StoneDBTables;
import sqlancer.stonedb.StoneDBToStringVisitor;
import sqlancer.stonedb.ast.StoneDBExpression;
import sqlancer.stonedb.ast.StoneDBJoin;
import sqlancer.stonedb.ast.StoneDBSelect;

public class StoneDBViewCreateGenerator {
    // the name of the view to create
    private final String viewName;
    private final StoneDBGlobalState globalState;
    private StoneDBSelect select;
    private final StringBuilder sb = new StringBuilder();

    public StoneDBViewCreateGenerator(StoneDBGlobalState globalState, String viewName) {
        this.globalState = globalState;
        this.viewName = viewName;
        setSelect();
    }

    public static SQLQueryAdapter generate(StoneDBGlobalState globalState, String viewName) {
        return new StoneDBViewCreateGenerator(globalState, viewName).getQuery();
    }

    private SQLQueryAdapter getQuery() {
        sb.append("CREATE ");
        sb.append("VIEW ");
        sb.append(viewName);
        sb.append(" AS ");
        sb.append(StoneDBToStringVisitor.asString(select));
        return new SQLQueryAdapter(sb.toString(), true);
    }

    private void setSelect() {
        StoneDBSchema schema = globalState.getSchema();
        StoneDBTables targetTables = schema.getRandomTableNonEmptyTables();
        StoneDBExpressionGenerator gen = new StoneDBExpressionGenerator(globalState)
                .setColumns(targetTables.getColumns());

        select = new StoneDBSelect();
        select.setFetchColumns(generateFetchColumns(targetTables));
        List<StoneDBTable> tables = targetTables.getTables();
        List<TableReferenceNode<StoneDBExpression, StoneDBTable>> tableReferenceNodeList = tables.stream()
                .map(t -> new TableReferenceNode<StoneDBExpression, StoneDBTable>(t)).collect(Collectors.toList());
        List<Node<StoneDBExpression>> joins = StoneDBJoin.getJoins(tableReferenceNodeList, globalState);
        select.setJoinList(new ArrayList<>(joins));
        select.setFromList(new ArrayList<>(tableReferenceNodeList));
        select.setWhereClause(gen.generateExpression());
        select.setOrderByClauses(gen.generateOrderBys());
        select.setGroupByExpressions(gen.generateExpressions(Randomly.smallNumber() + 1));
    }

    private List<Node<StoneDBExpression>> generateFetchColumns(StoneDBTables tables) {
        List<Node<StoneDBExpression>> columns = new ArrayList<>();
        if (Randomly.getBoolean()) {
            columns.add(new ColumnReferenceNode<>(new StoneDBColumn("*", null, false, false, 0)));
        } else {
            columns = Randomly.nonEmptySubset(tables.getColumns()).stream()
                    .map(c -> new ColumnReferenceNode<StoneDBExpression, StoneDBColumn>(c))
                    .collect(Collectors.toList());
        }
        return columns;
    }
}
